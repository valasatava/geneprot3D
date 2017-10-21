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
					.setMaster("local[" + 2 + "]")
					.setAppName("")
					.set("spark.driver.maxResultSize", "40g")
					.set("spark.executor.memory","40g")
					.set("spark.driver.memory","2g")
					.set("dfs.blocksize", blockSize.toString())
					.set("parquet.block.size", blockSize.toString() )
					.set("parquet.dictionary.page.size", blockSize.toString())
					.set("parquet.page.size", blockSize.toString())
					.set("spark.ui.showConsoleProgress", "true")
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.set("spark.kryoserializer.buffer.max", "2047mb")
					.set("spark.network.timeout", "1000000")
					.set("spark.storage.memoryFraction","0.3");
			sContext = new JavaSparkContext(conf);
			sContext.setCheckpointDir(DataLocationProvider.getDataHome());
		}
		return sContext;
	}
	
	public static SparkSession getSparkSession() {

		if (sparkSession==null) {
			sparkSession = SparkSession
					.builder()
					.master("local[" + 2 + "]")
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
