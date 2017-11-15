package org.rcsb.geneprot.genomemapping.loaders;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.MongoCollections;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.MapGenomeToUniProt;
import org.rcsb.geneprot.genomemapping.functions.MapTranscriptToIsoform;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadGenomeToUniprotMapping extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadGenomeToUniprotMapping.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static Dataset<Row> getTranscripts(String collectionName) {

        mongoDBOptions.put("spark.mongodb.input.collection", collectionName);
        JavaMongoRDD<Document> rdd = MongoSpark
                .load(new JavaSparkContext(sparkSession.sparkContext()), ReadConfig.create(sparkSession)
                        .withOptions(mongoDBOptions));

        Dataset<Row> df = rdd.withPipeline(
                Arrays.asList(Document.parse("{ $project: { "+
                                CommonConstants.COL_CHROMOSOME + ": \"$" + CommonConstants.COL_CHROMOSOME + "\", " +
                                CommonConstants.COL_GENE_NAME + ": \"$" + CommonConstants.COL_GENE_NAME + "\", " +
                                CommonConstants.COL_GENE_ID + ": \"$" + CommonConstants.COL_GENE_ID + "\", " +
                                CommonConstants.COL_ORIENTATION + ": \"$" + CommonConstants.COL_ORIENTATION + "\", " +
                                CommonConstants.COL_TRANSCRIPT_NAME + ": \"$" + CommonConstants.COL_TRANSCRIPT_NAME + "\", " +
                                CommonConstants.COL_TRANSCRIPT_ID + ": \"$" + CommonConstants.COL_TRANSCRIPT_ID + "\", " +
                                CommonConstants.COL_TRANSCRIPTION + ": \"$" + CommonConstants.COL_TRANSCRIPTION + "\", " +
                                CommonConstants.COL_UTR + ": \"$" + CommonConstants.COL_UTR + "\", " +
                                CommonConstants.COL_EXONS_COUNT + ": \"$" + CommonConstants.COL_EXONS_COUNT + "\", " +
                                CommonConstants.COL_EXONS + ": \"$" + CommonConstants.COL_EXONS + "\", " +
                                CommonConstants.COL_CODING + ": \"$" + CommonConstants.COL_CODING + "\", " +
                                CommonConstants.COL_HAS_ALTERNATIVE_EXONS + ": \"$" + CommonConstants.COL_HAS_ALTERNATIVE_EXONS + "\", " +
                                CommonConstants.COL_ALTERNATIVE_EXONS + ": \"$" + CommonConstants.COL_ALTERNATIVE_EXONS + "\" " +
                      " } }")))
                .toDF()
                .drop(col("_id"));

        //df = df.filter(col(CommonConstants.COL_GENE_NAME).equalTo("BIN1"));
        return df;
    }

    public static Dataset<Row> getUniProtMapping() throws Exception {

        String server = "ftp.uniprot.org";
        int port = 21;
        String user = "anonymous";
        String pass = "anonymous@141.161.180.197";
        String remote = "/pub/databases/uniprot/current_release/knowledgebase/idmapping//by_organism/HUMAN_9606_idmapping_selected.tab.gz";
        String download = "/Users/yana/Downloads/tmp.gz";

        //FTPDownloadFile.download(server, port, user, pass, remote, download);

        List<Row> records = sparkSession.sparkContext()
                .textFile(download, 200)
                .toJavaRDD()
                .map(line -> line.split("\\W"))
                .flatMap(new FlatMapFunction<String[],String[]>() {
                    @Override
                    public Iterator<String[]> call(String[] ss) throws Exception {
                        List<String[]> list = new ArrayList<>();
                        for (String s : ss) {
                            if (s.startsWith("ENST"))
                                list.add(new String[]{ss[0], s});
                        }
                        return list.iterator();
                    }
                })
                .map(row -> RowFactory.create(row))
                .collect();
        Dataset<Row> df = sparkSession.createDataFrame(records, CommonConstants.UNIPROT_TO_TRANSCRIPT_SCHEMA);

        return df;
    }

    public static Dataset<Row> mapTranscriptsToUniProtAccession(Dataset<Row> annotation) throws Exception {

        Dataset<Row> accessions = getUniProtMapping();
        annotation = annotation.join(accessions
                , annotation.col(CommonConstants.COL_TRANSCRIPT_ID)
                        .equalTo(accessions.col(CommonConstants.COL_TRANSCRIPT_ID))
                , "inner")
                .drop(accessions.col(CommonConstants.COL_TRANSCRIPT_ID));
        return annotation;
    }

    public static Dataset<Row> processTranscripts(Dataset<Row> transcripts) {

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        Broadcast<String> bc = jsc.broadcast(getOrganism());

        JavaRDD<Row> rdd = transcripts
                .toJavaRDD()
                .mapToPair(e -> new Tuple2<>(e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME))  + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_GENE_ID))     + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_GENE_NAME))   + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION)) + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)), e))
                .groupByKey()
                .flatMap(new MapTranscriptToIsoform(bc));

        List<Row> list = rdd.collect();
        StructType schema = list.get(0).schema();
        return sparkSession.createDataFrame(list, schema);
    }

    public static Dataset<Row> assembleTranscriptsAsGenes(Dataset<Row> transcripts) {

        try {
            transcripts = transcripts
                    .groupBy(col(CommonConstants.COL_CHROMOSOME), col(CommonConstants.COL_GENE_ID), col(CommonConstants.COL_GENE_NAME), col(CommonConstants.COL_ORIENTATION), col(CommonConstants.COL_UNIPROT_ACCESSION))
                    .agg(collect_list(
                            struct(   col(CommonConstants.COL_TRANSCRIPT_ID)
                                    , col(CommonConstants.COL_TRANSCRIPT_NAME)
                                    , col(CommonConstants.COL_TRANSCRIPTION)
                                    , col(CommonConstants.COL_UTR)
                                    , col(CommonConstants.COL_EXONS_COUNT)
                                    , col(CommonConstants.COL_EXONS)
                                    , col(CommonConstants.COL_CODING)
                                    , col(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)
                                    , col(CommonConstants.COL_ALTERNATIVE_EXONS)
                                    , col(CommonConstants.COL_MOLECULE_ID)
                                    , col(CommonConstants.COL_PROTEIN_SEQUENCE)
                                    , col(CommonConstants.COL_SEQUENCE_STATUS)
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
        String collectionNameTranscripts = MongoCollections.COLL_TRANSCRIPTS +"_"+ getTaxonomyId();
        Dataset<Row> transcripts = mapTranscriptsToUniProtAccession(getTranscripts(collectionNameTranscripts));

        transcripts = processTranscripts(transcripts);
        transcripts = assembleTranscriptsAsGenes(transcripts);

        List<GenomeToUniProtMapping> list = createMapping(transcripts);

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_MAPPING_UP + "_" + getTaxonomyId();
        ExternalDBUtils.writeListToMongo(list, collectionName);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}
