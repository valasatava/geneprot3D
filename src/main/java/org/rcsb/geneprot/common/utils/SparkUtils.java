package org.rcsb.geneprot.common.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import java.util.Properties;

public class SparkUtils {
	
	private static int cores = Runtime.getRuntime().availableProcessors();
	
	private static SparkSession sparkSession=null;
	
	private static SparkConf conf=null;
	private static JavaSparkContext sContext=null;

	private static final Properties props = org.rcsb.redwood.util.Parameters.getProperties();
	public static String MONGO_DB_IP = props.getProperty("mongodb.ip");
	public static String MONGO_DB_NAME = props.getProperty("mongodb.db.name");

	public static JavaSparkContext getSparkContext() {

		Integer blockSize = 1024 * 1024 * 1024;
		if (sContext==null) {
			conf = new SparkConf()
					.setMaster("local[" + cores + "]")
					.setAppName("")
					.set("spark.driver.maxResultSize", "24g")
					.set("spark.executor.memory","24g")
					.set("spark.driver.memory","8g")
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
					.master("local[" + cores + "]")
					.appName("app")
					.config("spark.driver.maxResultSize", "24g")
					.config("spark.executor.memory", "16g")
					.config("spark.debug.maxToStringFields", 80)
					.config("spark.mongodb.input.uri", "mongodb://"+MONGO_DB_IP+"/"+MONGO_DB_NAME+".default")
					.config("spark.mongodb.output.uri", "mongodb://"+MONGO_DB_IP+"/"+MONGO_DB_NAME+".default")
					.getOrCreate();
		}
		return sparkSession;
	}

	public static void stopSparkSession() {
		getSparkSession().stop();
	}
}
