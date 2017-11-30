package org.rcsb.geneprot.genomemapping.loaders;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.gencode.gtf.GTFParser;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.DatasetSchemas;
import org.rcsb.geneprot.genomemapping.constants.MongoCollections;
import org.rcsb.geneprot.genomemapping.functions.AnnotateAlternativeEvents;
import org.rcsb.geneprot.genomemapping.parsers.ParseGTFRecords;
import org.rcsb.redwood.util.DerivedDataLoadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.IllegalFormatException;
import java.util.List;

/** This loader process genome annotation file (in GTF format).
 *  This includes annotating alternative transcripts.
 *
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadCoreTranscripts extends AbstractLoader {

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static final Logger logger = LoggerFactory.getLogger(LoadCoreTranscripts.class);

    public static Dataset<Row> parseAnnotationFile(String filePath) throws IllegalFormatException {

        sparkSession.sparkContext().addFile(filePath);
        int n = filePath.split("/").length;
        String filename = SparkFiles.get(filePath.split("/")[n - 1]);

        JavaRDD<String> records = sparkSession.sparkContext()
                .textFile(filename, 200)
                .toJavaRDD();

        if (getFormat().equals("gtf")) {
            GTFParser parser = new GTFParser();
            JavaRDD<Row> rdd = records
                    .map(line -> parser.parseLine(line))
                    .filter(e -> e!= null)
                    .filter(e -> (e.getAttributes().containsKey("transcript_biotype")
                              && (e.getAttributes().get("transcript_biotype").equals("protein_coding"))))
                    .mapToPair(e -> new Tuple2<>(e.getAttributes().get("transcript_id"), e))
                    .groupByKey().map(t -> t._2)
                    .map(new ParseGTFRecords());

            Dataset<Row> df = sparkSession.createDataFrame(rdd, DatasetSchemas.GENCODE_TRANSCRIPT_SCHEMA);
            return df;

        } else {
            throw new IllegalArgumentException("The format entered, " + getFormat()+ " is not supported.");
        }
    }

    public static Dataset<Row> buildTranscripts() {

        try {
            String genomicAnnotationsFile = DataLocationProvider.getGenomeAnnotationResource(getTaxonomyId(), getFormat());
            Dataset<Row> transcripts = parseAnnotationFile(genomicAnnotationsFile);
            return transcripts;

        } catch (IllegalFormatException fe) {
            logger.error("This format is not supported {}", getFormat());

        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while building isoforms {} : {}", e.getCause(), e.getMessage());
        }
        return null;
    }

    public static Dataset<Row> processTranscripts(Dataset<Row> transcripts) {

        try {
            JavaRDD<Row> rdd = transcripts
                    .toJavaRDD()
                    .mapToPair(e -> new Tuple2<>( e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME)) + CommonConstants.KEY_SEPARATOR
                                                    + e.getString(e.fieldIndex(CommonConstants.COL_GENE_NAME))  + CommonConstants.KEY_SEPARATOR
                                                    + e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION)), e))
                    .groupByKey().map(e -> e._2)
                    .flatMap(new AnnotateAlternativeEvents());

            List<Row> list = rdd.collect();
            return sparkSession.createDataFrame(list, list.get(0).schema());

        } catch (Exception e) {
            logger.error("Exiting: fatal error has occurred while processing isoforms {} : {} {}", e.getCause(), e.getMessage(), e.fillInStackTrace());
        }
        return null;
    }

    public static void main(String[] args) throws Exception {

        logger.info("Started loading isoforms...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        Dataset<Row> transcripts = buildTranscripts();
        if (transcripts == null) {
            logger.error("ERROR: couldn't build transcripts");
            throw new Exception();
        }

        transcripts = processTranscripts(transcripts);
        if (transcripts == null) {
            logger.error("ERROR: couldn't process transcripts");
            throw new Exception();
        }

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_CORE_TRANSCRIPTS +"_"+ String.valueOf(getTaxonomyId());
        DerivedDataLoadUtils.writeToMongo(transcripts, collectionName, SaveMode.Overwrite);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}