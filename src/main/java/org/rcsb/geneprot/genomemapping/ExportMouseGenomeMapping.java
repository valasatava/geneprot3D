package org.rcsb.geneprot.genomemapping;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.geneprot.gencode.dao.MetadataDAO;
import org.rcsb.geneprot.genomemapping.functions.BuildGeneChromosomePosition;
import org.rcsb.geneprot.genomemapping.functions.GetUniprotGeneMapping;
import org.rcsb.humangenome.function.ChromosomeHaplotypeNameMap;
import org.rcsb.humangenome.function.ChromosomeNameFilter;
import org.rcsb.humangenome.function.SortByChromosomeName;
import org.rcsb.humangenome.function.SparkGeneChromosomePosition;
import org.rcsb.util.Parameters;
import org.rcsb.util.SparkUtils;
import org.rcsb.util.UniprotGeneMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


/**
 *
 */
public class ExportMouseGenomeMapping implements Serializable{

    private static final Logger logger = LoggerFactory.getLogger(ExportMouseGenomeMapping.class);

    private static String directoryName = "mousegenome";

    public JavaPairRDD<String, SparkGeneChromosomePosition> createChromosomeRDD(JavaSparkContext sc)
    {
        List<String> geneSymbols = null;
        try {
            geneSymbols = new ArrayList<>(MetadataDAO.getGeneSymbols().values());
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }

        JavaRDD<String> geneSymbolRDD = sc.parallelize(new ArrayList(geneSymbols));
        File f = new File("/Users/yana/spark/tmp/refFlat.txt.gz");
        BuildGeneChromosomePosition writer = new BuildGeneChromosomePosition(f);
        JavaPairRDD<String, SparkGeneChromosomePosition> chromoRDD = geneSymbolRDD.flatMapToPair(writer);

        logger.info("Got total chromoRDD count: " + chromoRDD.count());
        return chromoRDD;
    }

    private void writeChromosomesToFiles(JavaPairRDD<String, SparkGeneChromosomePosition> chromoRDD)
    {
        JavaRDD<String> chromoKeys = chromoRDD.keys().distinct();
        chromoKeys = chromoKeys.map(new ChromosomeHaplotypeNameMap()).distinct();
        chromoKeys = chromoKeys.sortBy(new SortByChromosomeName(),true,1);

        List<String> chromosomes = chromoKeys.collect();
        for (String c : chromosomes){
            System.out.println("PROCESSING CHROMOSOME " + c);
            writeChromosomeToFile(chromoRDD, c);
        }
    }

    private void writeChromosomeToFile(JavaPairRDD<String, SparkGeneChromosomePosition> chromoRDD, String chromosomeName)
    {
        ChromosomeNameFilter cnFilter = new ChromosomeNameFilter(chromosomeName);
        JavaPairRDD<String, SparkGeneChromosomePosition> genesPerChromosome = chromoRDD.filter(cnFilter).repartition(500);

        logger.info("Chromosome " + chromosomeName + " gene count: " + genesPerChromosome.count() + " " + genesPerChromosome.getNumPartitions());

        GetUniprotGeneMapping getUniprotGeneMapping = new GetUniprotGeneMapping();
        JavaPairRDD<String, UniprotGeneMapping> uniprotGeneMappingJavaRDD = genesPerChromosome.flatMapToPair(getUniprotGeneMapping).repartition(500);

        logger.info("Processing chromosome " + chromosomeName);

        JavaRDD<UniprotGeneMapping> rdds = uniprotGeneMappingJavaRDD.flatMap(new FlatMapFunction<Tuple2<String, UniprotGeneMapping>, UniprotGeneMapping>(){

            @Override
            public Iterator<UniprotGeneMapping> call(Tuple2<String, UniprotGeneMapping> s) throws Exception {
                List<UniprotGeneMapping> data  = new ArrayList<UniprotGeneMapping>();
                data.add(s._2());
                return data.iterator();
            }
        }).repartition(500);

        logger.info(rdds.count() + " UniProtGeneMaps on chromosome : " + chromosomeName);

        //writeAsORCFile(hiveContext,rdds, geneAssembly, chromosomeName);
        writeAsParquetFile(rdds, chromosomeName );
    }

    private void writeAsParquetFile(JavaRDD<UniprotGeneMapping> rdds, String chromosomeName)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyMMdd");
        String date = format.format(new Date());
        String path = Parameters.getWorkDirectory() + "/parquet/" + directoryName  +"/"+ date  + "/" + chromosomeName;

        long timeS = System.currentTimeMillis();

        Dataset<Row> dframe = SparkUtils.getSparkSession().createDataFrame(rdds, UniprotGeneMapping.class);
        Dataset<Row> df = dframe.withColumnRenamed("MRNAPos", "mRNAPos");

        logger.info("Writing results to " + path);
        df.write().mode(SaveMode.Overwrite).parquet(path);

        long timeE = System.currentTimeMillis();
        logger.info("time to writeAsParquetFile: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss"));

        // Writting schema
        File parent = new File(Parameters.getWorkDirectory()+"/schema/" + directoryName+"/"+ date);
        if (! parent.exists())
            parent.mkdirs();
        String schemaFile =  chromosomeName+".schema.txt";

        File f = new File(parent.getAbsolutePath() + "/" + schemaFile);
        try {
            if ( f.exists())
                f.delete();
            PrintWriter pw = new PrintWriter(f.getAbsolutePath());
            pw.write( df.schema().treeString() );
            pw.close();

        } catch (Exception e){
            e.printStackTrace();
        }

        logger.info("Completed saving rdd to folder: " + path);
    }

    public static void main(String[] args)
    {
        long timeS = System.currentTimeMillis();

        JavaSparkContext jsc = SparkUtils.getJavaSparkContext();
        SparkConf sparkConf = jsc.getConf();
        Class[] classArray = new Class[] {UniprotGeneMapping.class};
        sparkConf.registerKryoClasses(classArray);

        logger.info("Starting MouseGenomeMapping");

        SparkUtils.registerMysqlDriver(jsc);

        ExportMouseGenomeMapping me = new ExportMouseGenomeMapping();
        JavaPairRDD<String, SparkGeneChromosomePosition> chromRDD = me.createChromosomeRDD(jsc);

        //chromRDD.foreach(t-> System.out.println(t._1));
        me.writeChromosomesToFiles(chromRDD);



        jsc.close();
        jsc.stop();

        long timeE = System.currentTimeMillis();
        logger.info("Completed MouseGenomeMapping. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss"));

        System.exit(0);
    }
}
