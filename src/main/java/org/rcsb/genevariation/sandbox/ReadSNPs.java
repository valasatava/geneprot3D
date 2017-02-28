package org.rcsb.genevariation.sandbox;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Test class
 * 
 * @author Yana Valasatava
 */
public class ReadSNPs {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome + "/data/genevariation/mutations";
	
	public static void main(String[] args) throws IOException {
		
		int cores = Runtime.getRuntime().availableProcessors();
		SparkSession sparkSession = SparkSession
				.builder()
				.master("local[" + cores + "]")
				.appName("My Application")
				// the default in spark 2 seems to be 1g (see http://spark.apache.org/docs/latest/configuration.html) - JD 2016-10-06
				.config("spark.driver.maxResultSize", "4g")
				.config("spark.executor.memory", "4g")
				.config("spark.debug.maxToStringFields",80)
				.getOrCreate();
		
        Dataset<Row> mutations = sparkSession.read().parquet(path);
        mutations.createOrReplaceTempView("mutations");
        
        Dataset<Row> missense = mutations.filter(mutations.col("refAA").notEqual(mutations.col("mutAA")));
        missense.createOrReplaceTempView("missense");
        //missense.show();

	}
}
