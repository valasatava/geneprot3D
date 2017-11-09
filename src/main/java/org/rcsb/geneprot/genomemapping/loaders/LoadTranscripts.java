package org.rcsb.geneprot.genomemapping.loaders;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.MongoCollections;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.gencode.gtf.GTFParser;
import org.rcsb.geneprot.genomemapping.functions.AnnotateAlternativeEvents;
import org.rcsb.geneprot.genomemapping.parsers.ParseGTFRecords;
import org.rcsb.geneprot.genomemapping.parsers.ParseRefFlatRecords;
import org.rcsb.redwood.util.DerivedDataLoadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadTranscripts extends AbstractLoader {

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static final Logger logger = LoggerFactory.getLogger(LoadTranscripts.class);

    public static Dataset<Row> getTranscriptsAnnotation(String filePath, String format) {

        sparkSession.sparkContext().addFile(filePath);
        int n = filePath.split("/").length;
        String filename = SparkFiles.get(filePath.split("/")[n - 1]);

        JavaRDD<String> records = sparkSession.sparkContext()
                .textFile(filename, 200)
                .toJavaRDD();

        JavaRDD<Row> rdd = null;
        if (format.equals("refFlat")) {
            rdd = records.map(new ParseRefFlatRecords());

        } else if (format.equals("gtf")) {
            GTFParser parser = new GTFParser();
            rdd = records
                    .filter(e -> e.contains("protein_coding"))
                    .map(line -> parser.parseLine(line))
                    .mapToPair(e -> new Tuple2<>(e.getTranscriptId(), e))
                    .groupByKey()
                    .map(t -> t._2)
                    .map(new ParseGTFRecords());
        }

        Dataset<Row> df = sparkSession.createDataFrame(rdd, CommonConstants.GENOME_ANNOTATION_SCHEMA);
        return df;
    }

    public static Dataset<Row> buildTranscripts() {

        try {
            String genomicAnnotationsFile = DataLocationProvider.getGenomeAnnotationResource(getTaxonomyId(), "gtf");
            Dataset<Row> transcripts = getTranscriptsAnnotation(genomicAnnotationsFile, "gtf");
            transcripts = transcripts.filter(col(CommonConstants.COL_CDS_START).notEqual(col(CommonConstants.COL_CDS_END)));
            return transcripts;
        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while building transcripts {} : {}", e.getCause(), e.getMessage());
        }
        return null;
    }

    public static Dataset<Row> processTranscripts(Dataset<Row> transcripts) {

        try {
            JavaRDD<Row> rdd = transcripts
                    .toJavaRDD()
                    .mapToPair(e -> new Tuple2<>(e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME)) +
                            "_" + e.getString(e.fieldIndex(CommonConstants.COL_GENE_NAME)) +
                            "_" + e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION)), e))
                    .groupByKey()
                    .map(e -> e._2)
                    .flatMap(new AnnotateAlternativeEvents());
            List<Row> list = rdd.collect();
            StructType schema = list.get(0).schema();
            return sparkSession.createDataFrame(list, schema);
        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while processing transcripts {} : {}", e.getCause(), e.getMessage());
        }
        return null;
    }

    public static Dataset<Row> assembleTranscriptsAsGenes(Dataset<Row> transcripts) {
        try {
            transcripts = transcripts
                    .groupBy(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_NAME), col(CommonConstants.COL_ORIENTATION))
                    .agg(collect_list(
                            struct(   col(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)
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
            return transcripts;
        } catch (Exception e) {
            logger.error("Error has occurred while assembling transcripts as genes {} : {}", e.getCause(), e.getMessage());
        }
        return null;
    }

    public static void main(String[] args) {

        logger.info("Started loading transcripts...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        Dataset<Row> transcripts = buildTranscripts();
        if (transcripts == null)
            System.exit(1);

        transcripts = processTranscripts(transcripts);
        if (transcripts == null)
            System.exit(1);

        transcripts = assembleTranscriptsAsGenes(transcripts);
        if (transcripts == null)
            System.exit(1);

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_TRANSCRIPTS + getOrganism();
        DerivedDataLoadUtils.writeToMongo(transcripts, collectionName, SaveMode.Overwrite);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}