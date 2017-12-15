package org.rcsb.geneprot.genomemapping.loaders;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.MongoCollections;
import org.rcsb.geneprot.genomemapping.functions.MapGeneticToStructure;
import org.rcsb.mojave.genomemapping.MultipleFeaturesMap;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class LoadViewOnStructureToGenomicCoordinates extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadViewOnStructureToGenomicCoordinates.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static Dataset<Row> getTranscriptsToUniProtMapping() {

        mongoDBOptions.put("spark.mongodb.input.collection", MongoCollections.COLL_MAPPING_TRANSCRIPTS_TO_ISOFORMS + "_" + getTaxonomyId());
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));


        Dataset<Row> mapping = rdd.withPipeline(
                Arrays.asList(Document.parse("{ $project: { "+
                        CommonConstants.COL_CHROMOSOME + ": \"$" + CommonConstants.COL_CHROMOSOME + "\", " +
                        CommonConstants.COL_ORIENTATION + ": \"$" + CommonConstants.COL_ORIENTATION + "\" " +
                        CommonConstants.COL_GENE_ID + ": \"$" + CommonConstants.COL_GENE_ID + "\" " +
                        CommonConstants.COL_GENE_NAME + ": \"$" + CommonConstants.COL_GENE_NAME + "\" " +
                        CommonConstants.COL_TRANSCRIPT_ID + ": \"$" + CommonConstants.COL_TRANSCRIPT_ID + "\" " +
                        CommonConstants.COL_TRANSCRIPT_NAME + ": \"$" + CommonConstants.COL_TRANSCRIPT_NAME + "\" " +
                        CommonConstants.COL_UNIPROT_ACCESSION + ": \"$" + CommonConstants.COL_UNIPROT_ACCESSION + "\" " +
                        CommonConstants.COL_MOLECULE_ID + ": \"$" + CommonConstants.COL_MOLECULE_ID + "\" " +
                        CommonConstants.COL_CANONICAL + ": \"$" + CommonConstants.COL_CANONICAL + "\" " +
                        "coordinatesMapping" + ": \"$" + "coordinatesMapping" + "\" " +
                        " } }")))
                .toDF()
                .drop(col("_id"));

        // {"transcriptName":"BIN1-202", moleculeId:"O00499-1"}
        //mapping = mapping.filter(col(CommonConstants.COL_TRANSCRIPT_NAME).equalTo("BIN1-202").and(col(CommonConstants.COL_MOLECULE_ID).equalTo("O00499-1")));

        return mapping;
    }

    public static Dataset<Row> getEntityToUniProtMapping() {

        mongoDBOptions.put("spark.mongodb.input.collection", MongoCollections.COLL_MAPPING_ENTITIES_TO_ISOFORMS + "_" + getTaxonomyId());
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));

        Dataset<Row> mapping = rdd.withPipeline(
                Arrays.asList(
                        Document.parse("{ $project: { "+
                                CommonConstants.COL_ENTRY_ID + ": \"$" + CommonConstants.COL_ENTRY_ID + "\", " +
                                CommonConstants.COL_ENTITY_ID + ": \"$" + CommonConstants.COL_ENTITY_ID + "\", " +
                                CommonConstants.COL_CHAIN_ID + ": \"$" + CommonConstants.COL_CHAIN_ID + "\", " +
                                CommonConstants.COL_MOLECULE_ID + ": \"$" + CommonConstants.COL_MOLECULE_ID + "\", " +
                                "coordinatesMapping" + ": \"$" + "coordinatesMapping" + "\" " +
                                " } }")
                        ))
                .toDF()
                .drop(col("_id"));

        // {entryId:"1MV3", moleculeId:"O00499-1"}
        //mapping = mapping.filter(col(CommonConstants.COL_ENTRY_ID).equalTo("1MV3").and(col(CommonConstants.COL_MOLECULE_ID).equalTo("O00499-1")));

        return mapping;
    }

    public static List<MultipleFeaturesMap> getTranscriptsToEntityView() {

        Dataset<Row> df1 = getEntityToUniProtMapping()
                .withColumnRenamed("coordinatesMapping", "coordinatesMappingEntity");

        Dataset<Row> df2 = getTranscriptsToUniProtMapping()
                .withColumnRenamed("coordinatesMapping", "coordinatesMappingGenomic");

        Dataset<Row> df = df2
                .join(df1, df2.col(CommonConstants.COL_MOLECULE_ID).equalTo(df1.col(CommonConstants.COL_MOLECULE_ID)), "inner")
                .drop(df1.col(CommonConstants.COL_MOLECULE_ID));

        JavaRDD<MultipleFeaturesMap> rdd = df
                .toJavaRDD()
                .map(new MapGeneticToStructure())
                .filter(e -> e.getCoordinates().size()>0);
        List<MultipleFeaturesMap> results = rdd.collect();

        return results;
    }

    public static void main(String[] args) throws Exception {

        logger.info("Started loading genome to uniprot mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        logger.info("About to drop collection " + MongoCollections.VIEW_ON_EXONS_IN_3D +"_"+getTaxonomyId());
        ExternalDBUtils.dropCollection(MongoCollections.VIEW_ON_EXONS_IN_3D +"_"+getTaxonomyId());

        List<MultipleFeaturesMap> results = getTranscriptsToEntityView();
        ExternalDBUtils.writeListToMongo(results, MongoCollections.VIEW_ON_EXONS_IN_3D +"_"+getTaxonomyId());

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}
