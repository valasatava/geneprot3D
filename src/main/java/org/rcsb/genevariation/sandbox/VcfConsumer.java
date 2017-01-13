package org.rcsb.genevariation.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class VcfConsumer {
	
	public static void main(String[] args) {
		set();
	}
	public static void set() {
		
		final String userHome = System.getProperty("user.home");
		
		int cores = Runtime.getRuntime().availableProcessors();

        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("MapToPDB");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        
        DataFrame chr = sqlContext.read().parquet(userHome+"/data/genevariation/hg37/chr21");
        chr.registerTempTable("chr21");
        chr.show();
	}

}
