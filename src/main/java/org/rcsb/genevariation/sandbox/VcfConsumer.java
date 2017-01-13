package org.rcsb.genevariation.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class VcfConsumer {
	
	private final static String userHome = System.getProperty("user.home");
	private static int cores = Runtime.getRuntime().availableProcessors();
	
	private static SparkConf conf;
	private static JavaSparkContext sc;
	static SQLContext sqlContext;
	
	public static void main(String[] args) {
		
		setSpark();
		
		readUniprot();
		readSNP(21, 33031822);
		
	}
	
	public static void setSpark() {
		
        conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("MapToPDB");
        sc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sc);
	}
	
	public static void readSNP(int chr, int position) {
		
		readChromosome(chr);
		
		DataFrame snp = sqlContext.sql("select * from chr"+chr+" where position = "+position);
        snp.registerTempTable("snp");
        System.out.println("human genome mapping to UniProt for SNP:");
        snp.show();
		
	}
	
	public static void readChromosome(int chrn) {

        DataFrame chr = sqlContext.read().parquet(userHome+"/data/genevariation/hg37/chr"+chrn);
        chr.registerTempTable("chr"+chrn);
	}
	
	public static void readUniprot() {
		
		//register the Uniprot to PDB mapping
	    DataFrame uniprotPDB = sqlContext.read().parquet(userHome+"/data/genevariation/uniprotpdb/20161104");
	    uniprotPDB.registerTempTable("uniprotPDB");
	    uniprotPDB.show();
	}

}
