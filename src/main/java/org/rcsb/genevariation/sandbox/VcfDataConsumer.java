package org.rcsb.genevariation.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class VcfDataConsumer {
	
	private final static String userHome = System.getProperty("user.home");
	private static int cores = Runtime.getRuntime().availableProcessors();
	
	private static SparkConf conf;
	private static JavaSparkContext sc;
	static SQLContext sqlContext;
	
	public static void main(String[] args) {
		
		setSpark();
		
		readChromosome("21");
//		DataFrame snp = sqlContext.sql("select * from chr21 where position > 7900000").orderBy("position");
//      snp.registerTempTable("snp");
//      snp.show();	
       
		//readUniprot();
		//readSNP("21", 10413613);
		int phase = getPhaseSNP("21", 10413613);
		System.out.println(phase);
	}
	
	public static void setSpark() {
		
        conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName("MapToPDB");
        sc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sc);
	}
	
	public static void readSNP(String chr, long position) {
		
		DataFrame snp = sqlContext.sql("select * from chr"+chr+" where position="+position);
        snp.registerTempTable("snp");
        snp.show();
		
	}
	
	public static int getPhaseSNP(String chr, long position) {
		
		DataFrame snp = sqlContext.sql("select phase from chr"+chr+" where position="+position);
        snp.registerTempTable("snp");
        int phase = -2;
        Row[] rows = snp.collect();
        if (rows.length != 0) {
        	phase = (int) rows[0].get(0);
        }
        return phase;
	}
	
	public static void readChromosome(String chrn) {

        DataFrame chr = sqlContext.read().parquet(userHome+"/data/genevariation/hg38/chr"+chrn);
        chr.registerTempTable("chr"+chrn);
//      chr.show();
	}
	
	public static void readUniprot() {
		
		//register the Uniprot to PDB mapping
	    DataFrame uniprotPDB = sqlContext.read().parquet(userHome+"/data/genevariation/uniprotpdb/20161104");
	    uniprotPDB.registerTempTable("uniprotPDB");
//	    uniprotPDB.show();
	}

}
