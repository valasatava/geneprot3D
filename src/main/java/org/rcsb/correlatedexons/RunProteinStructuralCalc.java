package org.rcsb.correlatedexons;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.correlatedexons.mappers.MapToBestStructure;
import org.rcsb.genevariation.io.DataLocationProvider;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.List;

/**
 * Created by yana on 4/17/17.
 */
public class RunProteinStructuralCalc {

    private static int cores = Runtime.getRuntime().availableProcessors();
    private static SparkSession sparkSession = SparkSession
            .builder()
            .master("local[" + cores + "]")
            .appName("app")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.debug.maxToStringFields", 80)
            .getOrCreate();

    public static void run(String path) throws Exception {

//        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr1"};

        for (String chr : chromosomes) {

            Dataset<Row> map = sparkSession.read().parquet(path + "/" + chr);

            map.filter(map.col("geneName").equalTo("BROX").and(map.col("start").equalTo(222731357))).show();

            ClassTag<Dataset<Row>> uClassTag = ClassTag$.MODULE$.apply(Dataset.class);
            Broadcast<Dataset<Row>> mapbc = sparkSession.sparkContext().broadcast(map, uClassTag);

            JavaRDD<String> rdd = map.toJavaRDD()
                    .map(t -> (Integer.toString(t.getInt(4)) + "_" + Integer.toString(t.getInt(5)) + "_" + Integer.toString(t.getInt(7))))
                    .distinct();

            JavaPairRDD<String, Row> data = rdd.mapToPair(new MapToBestStructure(mapbc)).filter(t->t._2!=null);

            List<Tuple2<String, Row>> mapping = data.collect();
            for (Tuple2<String, Row> t : mapping) {
                System.out.println(t._2.toString());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        run(DataLocationProvider.getExonsStructuralMappingLocation());
    }
}
