package org.rcsb.genevariation.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SaprkUtils {
	
	private static int cores = Runtime.getRuntime().availableProcessors();
	
	private static SparkSession sparkSession; 
	
	private static SparkConf conf;
	private static JavaSparkContext sContext;
	
	public static JavaSparkContext getSparkContext() {
	    conf = new SparkConf()
	            .setMaster("local[" + cores + "]")
	            .setAppName("");
	    sContext = new JavaSparkContext(conf);
		return sContext;
	}
	
	public static SparkSession getSparkSession() {
		sparkSession = SparkSession
	            .builder()
	            .master("local[" + cores + "]")
	            .appName("app")
	            // the default in spark 2 seems to be 1g (see http://spark.apache.org/docs/latest/configuration.html) - JD 2016-10-06
	            .config("spark.driver.maxResultSize", "4g")
	            .config("spark.executor.memory", "4g")
	            .config("spark.debug.maxToStringFields",80)
	            .getOrCreate();
		return sparkSession;
	}
}
