package org.rcsb.geneprot.genomemapping.loaders;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.MongoCollections;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.MapGenomeToUniProt;
import org.rcsb.geneprot.genomemapping.functions.MapTranscriptToUniProtId;
import org.rcsb.geneprot.genomemapping.functions.MapTranscriptsToIsoforms;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.utils.MapperUtils;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadGenomeToUniprotMapping extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadGenomeToUniprotMapping.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static Dataset<Row> getTranscripts() {

        String collectionName = MongoCollections.COLL_TRANSCRIPTS + getOrganism();
        mongoDBOptions.put("spark.mongodb.input.collection", collectionName);
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));

        Dataset<Row> df = rdd.withPipeline(
                Arrays.asList(
                        Document.parse("{ $project: { "+
                                CommonConstants.COL_CHROMOSOME + ": \"$" + CommonConstants.COL_CHROMOSOME + "\", " +
                                CommonConstants.COL_GENE_NAME + ": \"$" + CommonConstants.COL_GENE_NAME + "\", " +
                                CommonConstants.COL_ORIENTATION + ": \"$" + CommonConstants.COL_ORIENTATION + "\", " +
                                CommonConstants.COL_TRANSCRIPTS + ": \"$" + CommonConstants.COL_TRANSCRIPTS + "\" " +
                                " } }")
                )
        ).toDF().drop(col("_id"));

        df = df.select(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_NAME), col(CommonConstants.COL_ORIENTATION)
                      , explode(col(CommonConstants.COL_TRANSCRIPTS)).as(CommonConstants.COL_TRANSCRIPT))
               .select(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_NAME), col(CommonConstants.COL_ORIENTATION)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION).as(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_TX_START).as(CommonConstants.COL_TX_START)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_TX_END).as(CommonConstants.COL_TX_END)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_CDS_START).as(CommonConstants.COL_CDS_START)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_CDS_END).as(CommonConstants.COL_CDS_END)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_EXONS_COUNT).as(CommonConstants.COL_EXONS_COUNT)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_EXONS_START).as(CommonConstants.COL_EXONS_START)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_EXONS_END).as(CommonConstants.COL_EXONS_END)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_HAS_ALTERNATIVE_EXONS).as(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)
                     , col(CommonConstants.COL_TRANSCRIPT).getField(CommonConstants.COL_ALTERNATIVE_EXONS).as(CommonConstants.COL_ALTERNATIVE_EXONS)
                );
        return df;
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

        Dataset<Row> organismIds = ExternalDBUtils.getAccessionsForOrganism(getTaxonomyId());
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

        Map<String, Iterable<String>> geneNamesMap = ExternalDBUtils.getGeneNameToUniProtAccessionsMap(getTaxonomyId())
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>(e.getString(e.schema().fieldIndex(CommonConstants.COL_GENE_NAME))
                        , e.getString(e.schema().fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION))))
                .groupByKey()
                .collectAsMap();
        return geneNamesMap;
    }

    public static Dataset<Row> processTranscripts(Dataset<Row> transcripts) {

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        Broadcast<Map<String, Row>> bcVar = jsc.broadcast(getVariationsMap());
        Broadcast<Map<String, Row>> bcSeq = jsc.broadcast(getSequenceFeaturesMap());
        Broadcast<Map<String, Iterable<String>>> bcGen = jsc.broadcast(getGeneNamesMap());

        JavaRDD<Row> rdd = transcripts
                .withColumn(CommonConstants.COL_MATCH, lit(true))
                .toJavaRDD()
                .flatMap(new MapTranscriptToUniProtId(bcGen))
                .mapToPair(e -> new Tuple2<>(e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME))+CommonConstants.KEY_SEPARATOR+
                                                 e.getString(e.fieldIndex(CommonConstants.COL_GENE_NAME))+CommonConstants.KEY_SEPARATOR+
                                                 e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION))+CommonConstants.KEY_SEPARATOR+
                                                 e.getString(e.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)), e))
                .groupByKey(100)
                .flatMap(new MapTranscriptsToIsoforms(bcSeq, bcVar));

        List<Row> list = rdd.collect();
        StructType schema = list.get(0).schema();
        return sparkSession.createDataFrame(list, schema);
    }

    public static Dataset<Row> assembleTranscriptsAsGenes(Dataset<Row> transcripts) {

        try {
            transcripts = transcripts
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
        return transcripts;
    }

    public static List<GenomeToUniProtMapping> createMapping(Dataset<Row> transcripts) {
        JavaRDD<GenomeToUniProtMapping> rdd = transcripts
                .toJavaRDD()
                .map(new MapGenomeToUniProt());
        List<GenomeToUniProtMapping> list = rdd.collect();
        return list;
    }

    public static void main(String[] args) throws Exception {

        logger.info("Started loading genome to uniprot mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);
        Dataset<Row> transcripts = MapperUtils.mapTranscriptsToUniProtAccession(getTranscripts());
        transcripts = processTranscripts(transcripts);
        transcripts = assembleTranscriptsAsGenes(transcripts);

        List<GenomeToUniProtMapping> list = createMapping(transcripts);

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_GENOME_TO_UNIPROT + getOrganism();
        ExternalDBUtils.writeListToMongo(list, collectionName);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}
