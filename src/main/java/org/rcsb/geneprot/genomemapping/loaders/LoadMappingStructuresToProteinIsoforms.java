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
import org.rcsb.geneprot.genomemapping.functions.MapStructureToProteinIsoformsCoordinates;
import org.rcsb.mojave.genomemapping.SequenceToStructureFeaturesMap;
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
public class LoadMappingStructuresToProteinIsoforms extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadViewOnStructureToGenomicCoordinates.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static Dataset<Row> getCurrentEntryIds() {

        mongoDBOptions.put("spark.mongodb.input.collection", org.rcsb.mojave.util.MongoCollections.COLL_ENTRY_ID_CURRENT);
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));

        Dataset<Row> df = rdd.withPipeline(
                Arrays.asList(Document.parse("{ $project: { "+ CommonConstants.COL_ENTRY_ID + ": \"$" + CommonConstants.COL_ENTRY_ID + "\" " + " } }")))
                .toDF().drop(col("_id"));

        return df;
    }

    public static List<SequenceToStructureFeaturesMap> getStructureToProteinIsoformsMapping() {

        Dataset<Row> df = getCurrentEntryIds();
        JavaRDD<SequenceToStructureFeaturesMap> rdd = df
                .toJavaRDD()
                .repartition(8000)
                .flatMap(new MapStructureToProteinIsoformsCoordinates());
        List<SequenceToStructureFeaturesMap> list = rdd.collect();

        return list;
    }

    public static void main(String[] args) throws Exception {

        logger.info("Started loading PDB structures to UniProt isoforms mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);
        List<SequenceToStructureFeaturesMap> list = getStructureToProteinIsoformsMapping();

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_MAPPING_ENTITIES_TO_ISOFORMS + "_" + getTaxonomyId();
        ExternalDBUtils.dropCollection(collectionName);
        ExternalDBUtils.writeListToMongo(list, collectionName);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}