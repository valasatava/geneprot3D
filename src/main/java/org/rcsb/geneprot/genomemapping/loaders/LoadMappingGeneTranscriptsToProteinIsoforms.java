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
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.DatasetSchemas;
import org.rcsb.geneprot.genomemapping.constants.MongoCollections;
import org.rcsb.geneprot.genomemapping.functions.MapGeneTranscriptsToProteinIsoforms;
import org.rcsb.geneprot.genomemapping.functions.MapGenomicToUniProtCoordinates;
import org.rcsb.geneprot.genomemapping.utils.FTPDownloadFile;
import org.rcsb.geneprot.genomemapping.utils.UniProtConnection;
import org.rcsb.mojave.genomemapping.TranscriptToSequenceFeaturesMap;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.util.*;

import static org.apache.spark.sql.functions.col;

/** This loader maps isoforms to UniProt isoform sequences.
 *
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadMappingGeneTranscriptsToProteinIsoforms extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadMappingGeneTranscriptsToProteinIsoforms.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static Dataset<Row> getTranscripts(String collectionName) {

        mongoDBOptions.put("spark.mongodb.input.partitionerOptions.numberOfPartitions", "200");
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
                                CommonConstants.COL_CODING + ": \"$" + CommonConstants.COL_CODING + "\" " +
                      " } }")))
                .toDF()
                .drop(col("_id"));

        return df;
    }

    public static Dataset<Row> getUniProtMapping() throws Exception {

        // TODO refactor this
        //String remote = UniProtConnection.getIdMappingLocation()+UniProtConnection.getHumanIdMappingFile();
        String remote = UniProtConnection.getIdMappingLocation()+UniProtConnection.getMouseIdMappingFile();
        String download = "/Users/yana/Downloads/tmp.gz";

        FTPDownloadFile.download( UniProtConnection.getServer(), UniProtConnection.getPort(),
                UniProtConnection.getUser(), UniProtConnection.getPass(), remote, download);

        List<Row> records = sparkSession.sparkContext()
                .textFile(download, 200)
                .toJavaRDD()
                .map(line -> line.split("\\W"))
                .flatMap(new FlatMapFunction<String[],String[]>() {
                    @Override
                    public Iterator<String[]> call(String[] ss) throws Exception {
                        List<String[]> list = new ArrayList<>();
                        for (String s : ss) {
                            // TODO: refactor!
                            //if (s.startsWith("ENST"))
                            if (s.startsWith("ENSM"))
                                list.add(new String[]{ss[0], s});
                        }
                        return list.iterator();
                    }
                })
                .map(row -> RowFactory.create(row))
                .collect();
        Dataset<Row> df = sparkSession.createDataFrame(records, DatasetSchemas.UNIPROT_TO_TRANSCRIPT_SCHEMA);

        File f = new File(download);
        if (f.exists())
            f.delete();

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
                .repartition(8000)
                .mapToPair(e -> new Tuple2<>(e.getString(e.fieldIndex(CommonConstants.COL_CHROMOSOME))  + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_GENE_ID))     + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_ORIENTATION)) + CommonConstants.KEY_SEPARATOR +
                                                 e.getString(e.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)), e))
                .groupByKey()
                .flatMap(new MapGeneTranscriptsToProteinIsoforms(bc));

        List<Row> list = rdd.filter( e -> e !=null ).collect();
        StructType schema = list.get(0).schema();

        return sparkSession.createDataFrame(list, schema);
    }

    public static List<TranscriptToSequenceFeaturesMap> createMapping(Dataset<Row> transcripts) {

        JavaRDD<TranscriptToSequenceFeaturesMap> rdd = transcripts
                .toJavaRDD()
                .repartition(800)
                .map(new MapGenomicToUniProtCoordinates());
        List<TranscriptToSequenceFeaturesMap> list = rdd.filter(e->e!=null).collect();
        return list;
    }

    public static void main(String[] args) throws Exception {

        logger.info("Started loading genome to uniprot mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        String collectionNameTranscripts = MongoCollections.COLL_CORE_TRANSCRIPTS +"_"+ getTaxonomyId();
        Dataset<Row> transcripts = mapTranscriptsToUniProtAccession(getTranscripts(collectionNameTranscripts));

        transcripts = processTranscripts(transcripts);
        List<TranscriptToSequenceFeaturesMap> list = createMapping(transcripts);

        logger.info("Writing mapping to a database");
        String collectionName = MongoCollections.COLL_MAPPING_TRANSCRIPTS_TO_ISOFORMS + "_" + getTaxonomyId();
        ExternalDBUtils.dropCollection(collectionName);
        ExternalDBUtils.writeListToMongo(list, collectionName);

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}