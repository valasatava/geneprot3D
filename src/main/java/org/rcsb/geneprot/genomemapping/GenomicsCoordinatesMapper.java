package org.rcsb.geneprot.genomemapping;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.MapGenomeToUniProt;
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
    private static JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

    private static int taxonomyId;
    private static String organism;
    private static Dataset<Row> organismIds;

    public static String getOrganism()
    {
        return organism;
    }

    public static void setOrganism(String organismName) {

        organism = organismName;
        if (organismName.equals("human"))
            taxonomyId = 9606;
        organismIds = ExternalDBUtils.getAccessionsForOrganism(taxonomyId).cache();
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

    public static Map<String, Row> getSequenceFeaturesMap() {

        Dataset<Row> df1 = ExternalDBUtils.getCanonicalSequenceFeatures();
        sparkSession.sqlContext().udf().register("toMoleculeId", (String s)->s+"-1", DataTypes.StringType);
        df1 = df1
                .withColumn(CommonConstants.COL_SEQUENCE_TYPE, lit("displayed"))
                .withColumn(CommonConstants.COL_FEATURE_ID, lit(null))
                .withColumn(CommonConstants.COL_MOLECULE_ID, callUDF("toMoleculeId", col(CommonConstants.COL_UNIPROT_ACCESSION)));
        Dataset<Row> df2 = ExternalDBUtils.getIsoformSequenceFeatures();
        Dataset<Row> df = df1.union(df2).dropDuplicates();


        df = df.join(organismIds, df.col(CommonConstants.COL_UNIPROT_ACCESSION).equalTo(organismIds.col(CommonConstants.COL_UNIPROT_ACCESSION)), "inner")
                .drop(organismIds.col(CommonConstants.COL_UNIPROT_ACCESSION));

        df = df.groupBy(col(CommonConstants.COL_UNIPROT_ACCESSION))
                .agg(collect_list(struct( col(CommonConstants.COL_MOLECULE_ID)
                        , col(CommonConstants.COL_PROTEIN_SEQUENCE)
                        , col(CommonConstants.COL_FEATURE_ID)
                        , col(CommonConstants.COL_SEQUENCE_TYPE))).as(CommonConstants.COL_FEATURES));

        Map<String, Row> map = df
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>(e.getString(e.schema().fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)), e))
                .collectAsMap();
        return map;
    }

    public static Map<String, Iterable<String>> getGeneNamesMap() {

        Map<String, Iterable<String>> geneNamesMap = ExternalDBUtils.getGeneNameToUniProtAccessionsMap(taxonomyId)
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>( e.getString(e.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME))
                        , e.getString(e.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION))))
                .groupByKey()
                .collectAsMap();
        return geneNamesMap;
    }

    public static Dataset<Row> getTranscriptsAnnotation(String filePath) {

        sparkSession.sparkContext().addFile(filePath);
        int n = filePath.split("/").length;
        String filename = SparkFiles.get(filePath.split("/")[n - 1]);

        StructType schema = CommonConstants.GENOME_ANNOTATION_SCHEMA;
        schema.fieldIndex(CommonConstants.COL_GENE_NAME);

        JavaRDD<Row> rdd =
                sparkSession.sparkContext().textFile(filename, 200)
                        .toJavaRDD()
                        .map(t -> t.split(CommonConstants.FIELD_SEPARATOR))
                        .map(t -> RowFactory.create(
                                t[schema.fieldIndex(CommonConstants.COL_GENE_NAME)]
                                , t[schema.fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)]
                                , t[schema.fieldIndex(CommonConstants.COL_CHROMOSOME)]
                                , t[schema.fieldIndex(CommonConstants.COL_ORIENTATION)]
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_EXONS_COUNT)])
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_START)]
                                        .split(CommonConstants.EXONS_FIELD_SEPARATOR))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_END)]
                                        .split(Pattern.quote(",")))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                )
                        );

        Dataset<Row> df = sparkSession.createDataFrame(rdd, CommonConstants.GENOME_ANNOTATION_SCHEMA);
        return df;
    }

    public static Dataset<Row> buildTranscripts() throws Exception {

        Dataset<Row> transcripts = getTranscriptsAnnotation(DataLocationProvider.getHumanGenomeAnnotationResourceFromUCSC());
        transcripts = transcripts.filter(col(CommonConstants.COL_CDS_START).notEqual(col(CommonConstants.COL_CDS_END)));
        transcripts = MapperUtils.mapTranscriptsToUniProtAccession(transcripts);
        transcripts = transcripts.withColumn(CommonConstants.COL_MATCH, lit(true));

//        transcripts = transcripts
//                .filter(col(CommonConstants.COL_GENE_NAME).equalTo("AMY1A")
//                //.and(col(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION).equalTo("NM_001286828"))
//        );
//        transcripts.show();

        return transcripts.cache();
    }

//    public static Dataset<Row> processTranscripts(Dataset<Row> transcripts) throws Exception {
//
//        Broadcast<Map<String, Row>> bcVar = jsc.broadcast(getVariationsMap());
//        Broadcast<Map<String, Row>> bcSeq = jsc.broadcast(getSequenceFeaturesMap());
//        Broadcast<Map<String, Iterable<String>>> bcGen = jsc.broadcast(getGeneNamesMap());
//
//        JavaRDD<Row> rdd = transcripts
//                .toJavaRDD()
//                .flatMap(new MapTranscriptToUniProtId(bcGen))
//                .mapToPair(e -> new Tuple2<>(e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME))+
//                                        "_"+e.getString(e.fieldIndex(CommonConstants.COL_GENE_NAME))+
//                                        "_"+e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION)), e))
//                .groupByKey(10).map(e -> e._2)
//                //.map(new AnnotateAlternativeEvents())
//                .flatMap(new MapTranscriptsToIsoforms(bcSeq, bcVar));
//
//        List<Row> list = rdd.collect();
//        StructType schema = list.get(0).schema();
//        return sparkSession.createDataFrame(list, schema);
//    }

    public static Dataset<Row> assembleTranscriptsAsGenes(Dataset<Row> isoforms) {

        try {
            isoforms = isoforms
                    .filter(col(CommonConstants.COL_MOLECULE_ID).isNotNull())
                    .groupBy(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_NAME), col(CommonConstants.COL_ORIENTATION), col(CommonConstants.COL_UNIPROT_ACCESSION))
                    .agg(collect_list(
                            struct(   col(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)
                                    , col(CommonConstants.COL_NCBI_PROTEIN_SEQUENCE_ACCESSION)
                                    , col(CommonConstants.COL_MOLECULE_ID)
                                    , col(CommonConstants.COL_ISOFORM_ID)
                                    , col(CommonConstants.COL_MATCH)
                                    , col(CommonConstants.COL_TX_START)
                                    , col(CommonConstants.COL_TX_END)
                                    , col(CommonConstants.COL_CDS_START)
                                    , col(CommonConstants.COL_CDS_END)
                                    , col(CommonConstants.COL_EXONS_COUNT)
                                    , col(CommonConstants.COL_EXONS_START)
                                    , col(CommonConstants.COL_EXONS_END)
                                    , col(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)
                                    , col(CommonConstants.COL_ALTERNATIVE_EXONS)
                            )).as(CommonConstants.COL_TRANSCRIPTS))
                    .sort(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_NAME));
        } catch (Exception e) {
            logger.error("Error has occurred while assembling transcripts as genes {} : {}", e.getCause(), e.getMessage());
        }
        return isoforms;
    }

    public static void main(String[] args) {

        logger.info("Genome mapping is started");
        long timeS = System.currentTimeMillis();

        setOrganism("human");
        Dataset<Row> transcripts=null;
        try {
            //transcripts = processTranscripts(buildTranscripts());
            transcripts = assembleTranscriptsAsGenes(transcripts);
        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while building the transcripts {} : {}", e.getCause(), e.getMessage());
        }

        try {
            if (transcripts != null) {
                JavaRDD<GenomeToUniProtMapping> rdd = transcripts
                        .toJavaRDD()
                        .map(new MapGenomeToUniProt());
                List<GenomeToUniProtMapping> list = rdd.collect();

                logger.info("Writing mapping to a database");
                ExternalDBUtils.writeListToMongo(list, "");
            }

        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while mapping to uniprot {} : {}", e.getCause(), e.getMessage());
        }
        finally {
            jsc.stop();
            sparkSession.stop();
        }
        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}