package org.rcsb.geneprot.genomemapping;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.BuildAlternativeTranscripts;
import org.rcsb.geneprot.genomemapping.functions.MapGenomeToUniProt;
import org.rcsb.geneprot.genomemapping.functions.MapToGenomeSequence;
import org.rcsb.geneprot.genomemapping.functions.MapTranscriptToIsoform;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.utils.MapperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
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

    public static String getOrganism() {
        return organism;
    }
    public static void setOrganism(String organism) {
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
                sparkSession.sparkContext().textFile(filename, 100)
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

    public static Dataset<Row> annotateMissingIsoforms(Dataset<Row> transcripts) throws Exception
    {
        JavaPairRDD<Row, String> rdd = transcripts
                .toJavaRDD()
                .mapToPair(new BuildAlternativeTranscripts())
                .mapValues(new MapToGenomeSequence(getOrganism()))
                .mapToPair(new MapTranscriptToIsoform());
        rdd.collect();
        return null;
    }

    public static Dataset<Row> annotateMissingProteins(Dataset<Row> transcripts)
    {
        return null;
    }

    public static Dataset<Row> getAlternativeProducts() throws Exception
    {
        logger.info("Getting alternative transcripts...");

        Dataset<Row> transcripts = getTranscriptsAnnotation(DataLocationProvider.getHumanGenomeAnnotationResource());
        transcripts = MapperUtils.mapTranscriptsToUniProtAccession(transcripts).cache();

        Dataset<Row> assigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNotNull()))
                .cache();

        Dataset<Row> missing = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNull()))
                .cache();

        Dataset<Row> notassigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNull()
                        .and(col(CommonConstants.MOLECULE_ID).isNull()))
                .cache();

        Dataset<Row> isoforms = assigned
                .union(annotateMissingIsoforms(missing))
                .union(annotateMissingProteins(notassigned));

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
        //transcripts = transcripts.filter(col(CommonConstants.GENE_NAME).equalTo("MAGI3"));

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