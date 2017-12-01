package org.rcsb.geneprot.genomemapping.loaders;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.MongoCollections;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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

        Dataset<Row> df = rdd.withPipeline(
                Arrays.asList(Document.parse("{ $project: { "+
                        CommonConstants.COL_UNIPROT_ACCESSION + ": \"$" + CommonConstants.COL_UNIPROT_ACCESSION + "\", " +
                        CommonConstants.COL_ISOFORMS+ ": \"$" + CommonConstants.COL_TRANSCRIPTS + "\" " +
                        " } }")))
                .toDF()
                .drop(col("_id"));

        return df;
    }

    public static Dataset<Row> getEntityToUniProtMapping() {

        mongoDBOptions.put("spark.mongodb.input.collection", org.rcsb.mojave.util.MongoCollections.COLL_ENTITY_TO_UNIPROT_MAPPING);
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));

        Dataset<Row> mapping = rdd.withPipeline(
                Arrays.asList(
                        Document.parse("{ $project: { "+
                                org.rcsb.mojave.util.CommonConstants.COL_ENTRY_ID + ": \"$" + org.rcsb.mojave.util.CommonConstants.COL_ENTRY_ID + "\", " +
                                org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ": { \"$setUnion\": [ \"$" + org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_TO_PDB_MAPPING
                                + "." + org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + "\", [] ] }"+
                                " } }")
                        ,  Document.parse("{ $unwind: { path:" + " \"$" + org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION +"\", "+
                                "preserveNullAndEmptyArrays : true" +
                                " } }")))
                .toDF()
                .drop(col("_id"))
                .dropDuplicates();

        return mapping;
    }

    public static Dataset<Row> getTranscriptsToEntityView() {

        Dataset<Row> df1 = getEntityToUniProtMapping();
        Dataset<Row> df2 = getTranscriptsToUniProtMapping();

        Dataset<Row> df = df2
                .join(df1
                    , df2.col(CommonConstants.COL_UNIPROT_ACCESSION)
                                .equalTo(df1.col(CommonConstants.COL_UNIPROT_ACCESSION))
                    , "inner")
                .drop(df1.col(CommonConstants.COL_UNIPROT_ACCESSION));
        return df;
    }

    public static Dataset<Row> mapToPdbCoordinates(Dataset<Row> df) {


        return null;
    }
    
    public static void main(String[] args) {

        logger.info("Started loading genome to uniprot mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        Dataset<Row> df = getTranscriptsToEntityView();
        df = mapToPdbCoordinates(df);
        df.show();

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}
