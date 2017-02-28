package org.rcsb.genevariation.sandbox;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Test class
 * 
 * @author Yana Valasatava
 */
public class ReadSNPsParquet {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome + "/data/genevariation/mutations";
	
	public static void main(String[] args) throws IOException {

        Dataset<Row> mutations = SaprkUtils.getSparkSession().read().parquet(path);
        mutations.createOrReplaceTempView("mutations");
        
        Dataset<Row> missense = mutations.filter(mutations.col("refAA").notEqual(mutations.col("mutAA")));
        missense.createOrReplaceTempView("missense");
        missense.show();
	}
}
