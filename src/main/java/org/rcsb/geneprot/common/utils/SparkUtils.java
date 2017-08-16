package org.rcsb.geneprot.common.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.rcsb.geneprot.common.io.DataLocationProvider;

public class SparkUtils {
	
	private static int cores = Runtime.getRuntime().availableProcessors();
	
	private static SparkSession sparkSession=null;
	
	private static SparkConf conf=null;
	private static JavaSparkContext sContext=null;
	
	public static JavaSparkContext getSparkContext() {

		Integer blockSize = 1024 * 1024 * 1024;
		if (sContext==null) {
			conf = new SparkConf()
					.setMaster("local[" + cores + "]")
					.setAppName("")
					.set("parquet.block.size", blockSize.toString() )
					.set("parquet.dictionary.page.size", blockSize.toString())
					.set("parquet.page.size", blockSize.toString())
					.set("spark.executor.memory","8g")
					.set("spark.driver.memory","4g")
					.set("spark.driver.maxResultSize", "8g");
			sContext = new JavaSparkContext(conf);
			sContext.setCheckpointDir(DataLocationProvider.getDataHome());
		}
		return sContext;
	}
	
	public static SparkSession getSparkSession() {

		if (sparkSession==null) {
			sparkSession = SparkSession
					.builder()
					.master("local[" + cores + "]")
					.appName("app")
					.config("spark.driver.maxResultSize", "4g")
					.config("spark.executor.memory", "4g")
					.config("spark.debug.maxToStringFields", 80)
					.getOrCreate();
		}
		return sparkSession;
	}

	public static void stopSparkSession() {
		getSparkSession().stop();
	}
}
