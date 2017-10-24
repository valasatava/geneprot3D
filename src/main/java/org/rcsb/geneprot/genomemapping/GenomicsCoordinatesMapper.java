package org.rcsb.geneprot.genomemapping;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.*;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.utils.MapperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class GenomicsCoordinatesMapper {

    private static final Logger logger = LoggerFactory.getLogger(GenomicsCoordinatesMapper.class);
    private static SparkSession sparkSession = SparkUtils.getSparkSession();

    private static String organism;

    public static String getOrganism()
    {
        return organism;
    }

    public static void setOrganism(String organism)
    {
        GenomicsCoordinatesMapper.organism = organism;
    }

    public static Dataset<Row> getTranscriptsAnnotation(String filePath)
    {
        sparkSession.sparkContext().addFile(filePath);
        int n = filePath.split("/").length;
        String filename = SparkFiles.get(filePath.split("/")[n - 1]);

        StructType schema = CommonConstants.GENOME_ANNOTATION_SCHEMA;
        schema.fieldIndex(CommonConstants.GENE_NAME);

        JavaRDD<Row> rdd =
                sparkSession.sparkContext().textFile(filename, 200)
                        .toJavaRDD()
                        .map(t -> t.split(CommonConstants.FIELD_SEPARATOR))
                        .map(t -> RowFactory.create(
                                t[schema.fieldIndex(CommonConstants.GENE_NAME)]
                                , t[schema.fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)]
                                , t[schema.fieldIndex(CommonConstants.CHROMOSOME)]
                                , t[schema.fieldIndex(CommonConstants.ORIENTATION)]
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.TX_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.TX_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.CDS_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.CDS_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.EXONS_COUNT)])
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.EXONS_START)]
                                        .split(CommonConstants.EXONS_FIELD_SEPARATOR))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.EXONS_END)]
                                        .split(Pattern.quote(",")))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                )
                        );

        Dataset<Row> df = sparkSession.createDataFrame(rdd, CommonConstants.GENOME_ANNOTATION_SCHEMA);
        return df;
    }

    public static Dataset<Row> assignMissingProteins(Dataset<Row> transcripts)
    {
        Map<String, Iterable<String>> geneNamesMap = ExternalDBUtils.getGeneNameToUniProtAccessionsMap()
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>( e.getString(e.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME))
                                            , e.getString(e.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION))))
                .groupByKey()
                .collectAsMap();

        Broadcast<Map<String, Iterable<String>>> bc = new JavaSparkContext(sparkSession.sparkContext()).broadcast(geneNamesMap);
        JavaRDD<Row> rdd = transcripts
                .toJavaRDD()
                .flatMap(new MapTranscriptToUniProtId(bc))
                .map(new UpdateRow());

        return sparkSession.createDataFrame(rdd, transcripts.schema());
    }

    public static Map<String, Row> getVariationsMap() {

        Dataset<Row> df1 = ExternalDBUtils.getSequenceVariationsInRanges();
        df1 = df1.withColumn(CommonConstants.COL_POSITION, lit(null))
                .select(CommonConstants.COL_FEATURE_ID, CommonConstants.COL_VARIATION,
                        CommonConstants.COL_ORIGINAL, CommonConstants.COL_POSITION,
                        CommonConstants.COL_BEGIN, CommonConstants.COL_END);
        Dataset<Row> df2 = ExternalDBUtils.getSinglePositionVariations();
        df2 = df2.withColumn(CommonConstants.COL_BEGIN, lit(null))
                .withColumn(CommonConstants.COL_END, lit(null))
                .select(CommonConstants.COL_FEATURE_ID, CommonConstants.COL_VARIATION,
                        CommonConstants.COL_ORIGINAL, CommonConstants.COL_POSITION,
                        CommonConstants.COL_BEGIN, CommonConstants.COL_END);
        Dataset<Row> df = df1.union(df2);

        Map<String, Row> map = df
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>(e.getString(e.schema().fieldIndex(CommonConstants.COL_FEATURE_ID)), e))
                .collectAsMap();
        return map;
    }

    public static Dataset<Row> assignMissingIsoforms(Dataset<Row> transcripts) throws Exception
    {

        Map<String, Row> map = getVariationsMap();
        Broadcast<Map<String, Row>> bc = new JavaSparkContext(sparkSession.sparkContext()).broadcast(map);
        JavaRDD<Row> rdd = transcripts
                .toJavaRDD()
                .mapToPair(new BuildAlternativeTranscripts())
                .repartition(10)
                .map(new MapTranscriptToIsoform(getOrganism(), bc)).map(new UpdateRow())
                .repartition(200)
                .map(new MapTranscriptToIsoformId()).map(new UpdateRow());

        return sparkSession.createDataFrame(rdd, transcripts.schema());
    }

    public static Dataset<Row> getAlternativeProducts() throws Exception
    {
        logger.info("Getting alternative transcripts...");

        Dataset<Row> transcripts = getTranscriptsAnnotation(DataLocationProvider.getHumanGenomeAnnotationResource());
        transcripts = MapperUtils.mapTranscriptsToUniProtAccession(transcripts).cache();

        // TRANSCRIPTS ASSIGNED TO ISOFORMS
        Dataset<Row> assigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNotNull())).cache();

        // ISOFORM ID ASSIGNMENT IS MISSING - MAP VIA BUILDING ISOFORM SEQUENCES
        Dataset<Row> missing = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNull())).cache();

        // UNIPROT ID ASSIGNMENT IS MISSING - MAP VIA GENE NAME
        Dataset<Row> notassigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNull())).cache();

        // ASSIGNING UNIPROT ID
        Dataset<Row> recovered = assignMissingProteins(notassigned)
                        .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull());
        missing = missing.union(recovered);

        // ASSIGNING ISOFORM ID
        Dataset<Row> isoforms = assigned
                .union(assignMissingIsoforms(missing))
                .filter(col(CommonConstants.MOLECULE_ID).isNotNull());
        isoforms.show();

        isoforms = isoforms
                .filter(col(CommonConstants.MOLECULE_ID).isNotNull()) // CHECK ISSUES WITH DB
                .groupBy(col(CommonConstants.CHROMOSOME), col(CommonConstants.GENE_NAME), col(CommonConstants.ORIENTATION), col(CommonConstants.UNIPROT_ID))
                .agg(collect_list(
                        struct(   col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)
                                , col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION)
                                , col(CommonConstants.MOLECULE_ID)
                                , col(CommonConstants.ISOFORM_ID)
                                , col(CommonConstants.TX_START)
                                , col(CommonConstants.TX_END)
                                , col(CommonConstants.CDS_START)
                                , col(CommonConstants.CDS_END)
                                , col(CommonConstants.EXONS_COUNT)
                                , col(CommonConstants.EXONS_START)
                                , col(CommonConstants.EXONS_END)
                        )).as(CommonConstants.TRANSCRIPTS))
                .sort(col(CommonConstants.CHROMOSOME), col(CommonConstants.GENE_NAME));

        return isoforms;
    }

    public static void main(String[] args) throws Exception {

        setOrganism("human");
        Dataset<Row> transcripts = getAlternativeProducts();

        JavaRDD<GenomeToUniProtMapping> rdd = transcripts
                .toJavaRDD()
                .map(new MapGenomeToUniProt());

        List<GenomeToUniProtMapping> list = rdd.collect();
        ExternalDBUtils.writeListToMongo(list);

//        Dataset<Row> dataset = sparkSession.createDataset(rdd, CommonConstants.GENOME_MAPPING_SCHEMA);
//        dataset.persist(StorageLevel.MEMORY_AND_DISK());
//        dataset.printSchema();
//        DerivedDataLoadUtils.writeToMongo(dataset, "humanGenomeMapping", SaveMode.Overwrite);
    }
}