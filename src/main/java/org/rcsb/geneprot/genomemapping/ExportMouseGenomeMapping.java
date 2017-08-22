package org.rcsb.geneprot.genomemapping;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.BuildGeneChromosomePosition;
import org.rcsb.geneprot.genomemapping.functions.GetUniprotGeneMapping;
import org.rcsb.geneprot.genomemapping.functions.SortByChromosomeName;
import org.rcsb.humangenome.function.ChromosomeHaplotypeNameMap;
import org.rcsb.humangenome.function.ChromosomeNameFilter;
import org.rcsb.humangenome.function.SparkGeneChromosomePosition;
import org.rcsb.util.Parameters;
import org.rcsb.util.UniprotGeneMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;


/**
 *
 */
public class ExportMouseGenomeMapping implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ExportMouseGenomeMapping.class);

    private static String directoryName = "mousegenome";

    public JavaPairRDD<String, SparkGeneChromosomePosition> createChromosomeRDD()
    {

        String genomicPositionsFile = "/Users/yana/spark/tmp/refFlat.txt.gz";
        InputStream instream = null;
        try { instream = new GZIPInputStream(new FileInputStream(new File(genomicPositionsFile)));
        } catch (IOException e) {
           logger.error("File with genomic positions is not found");
        }

        List<GeneChromosomePosition> chromosomePositions = null;
        GeneChromosomePositionParser parser = new GeneChromosomePositionParser();
        try { chromosomePositions = parser.getChromosomeMappings(instream);
        } catch (IOException e) {
            logger.error("Cannot get chromosome positions");
        }

        JavaPairRDD<String, SparkGeneChromosomePosition> chromosomeRDD = SparkUtils.getSparkContext()
                .parallelize(chromosomePositions).mapToPair(new BuildGeneChromosomePosition()).cache();
        logger.info("Got total chromoRDD count: " + chromosomeRDD.count());

        return chromosomeRDD;
    }

    private void writeChromosomesToFiles(JavaPairRDD<String, SparkGeneChromosomePosition> chromoRDD) throws Exception
    {
        JavaRDD<String> chromoKeys = chromoRDD.map(t->t._1).distinct();
        chromoKeys = chromoKeys.map(new ChromosomeHaplotypeNameMap()).distinct();
        chromoKeys = chromoKeys.sortBy(new SortByChromosomeName(),true,1);

        List<String> chromosomes = chromoKeys.collect();
        for (String c : chromosomes){
            logger.info("Processing chromosome " + c);
            writeChromosomeToFile(chromoRDD, c);
        }
    }

    private void writeChromosomeToFile(JavaPairRDD<String, SparkGeneChromosomePosition> chromoRDD, String chromosomeName) throws Exception
    {
        ChromosomeNameFilter cnFilter = new ChromosomeNameFilter(chromosomeName);
        JavaPairRDD<String, SparkGeneChromosomePosition> genesPerChromosome = chromoRDD.filter(cnFilter);

        GetUniprotGeneMapping getUniprotGeneMapping = new GetUniprotGeneMapping();
        JavaPairRDD<String, UniprotGeneMapping> uniprotGeneMappingJavaRDD = genesPerChromosome.flatMapToPair(getUniprotGeneMapping).repartition(500);

        JavaRDD<UniprotGeneMapping> rdds = uniprotGeneMappingJavaRDD.map(t -> t._2);
        writeAsParquetFile(rdds, chromosomeName);
    }

    private void writeAsParquetFile(JavaRDD<UniprotGeneMapping> rdds, String chromosomeName)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyMMdd");
        String date = format.format(new Date());
        String path = Parameters.getWorkDirectory() + "/parquet/" + directoryName  +"/"+ date  + "/" + chromosomeName;

        long timeS = System.currentTimeMillis();

        Dataset<Row> dframe = SparkUtils.getSparkSession().createDataFrame(rdds, UniprotGeneMapping.class)
                .withColumnRenamed("MRNAPos", "mRNAPos").persist(StorageLevel.MEMORY_AND_DISK());

        //System.out.println(dframe.count());

        logger.info("Writing results to " + path);
        dframe.write().mode(SaveMode.Overwrite).parquet(path);
        dframe.unpersist();

        long timeE = System.currentTimeMillis();
        logger.info("time to writeAsParquetFile: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss"));

        logger.info("Completed saving rdd to folder: " + path);
    }

    public static void main(String[] args) throws Exception
    {
        long timeS = System.currentTimeMillis();

        logger.info("Starting MouseGenomeMapping");

        ExportMouseGenomeMapping me = new ExportMouseGenomeMapping();
        JavaPairRDD<String, SparkGeneChromosomePosition> chromosomeRDD = me.createChromosomeRDD();

        me.writeChromosomesToFiles(chromosomeRDD);

        SparkUtils.getSparkContext().stop();

        long timeE = System.currentTimeMillis();
        logger.info("Completed MouseGenomeMapping. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss"));

        System.exit(0);
    }
}