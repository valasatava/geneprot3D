package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

public class PDBDfDataProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String dfPath = userHome + "/data/genevariation/hg38/";
	
	public static Dataset<Row> readChromosome(int n) {
		
		String chrname = "chr"+Integer.toString(n);
        Dataset<Row> chr = SaprkUtils.getSparkSession().read().parquet(dfPath+chrname);
        return chr.filter("chromosome='"+chrname+"'").orderBy("position");
	}
	
	public static void main(String[] args) {
		
		Dataset<Row> chr = readChromosome(17);
		chr.show();
		
//		DataFrame snp = sqlContext.sql("select * from chr1"
//				+ " where position = 18849").orderBy("position");
//        snp.registerTempTable("snp");
//        snp.show();
       
		//readUniprot();
		//readSNP("21", 10413613);
		//int phase = getPhaseSNP("21", 10413613);
		//System.out.println(phase);
	}

	
//	public static void readSNP(String chr, long position) {
//		
//		DataFrame snp = sqlContext.sql("select * from chr"+chr+" where position="+position);
//        snp.registerTempTable("snp");
//        snp.show();
//		
//	}
	
//	public static SNP getSNPData(String chr, long position) {
//		
//		DataFrame snp = sqlContext.sql("select chromosome, phase, orientation, inCoding, uniProtId from chr"+chr+" where position="+position);
//        snp.registerTempTable("snp");
//        
//        SNP snpData=null;
//        Row[] rows = snp.collect();
//        if (rows.length != 0) {
//        	String chromosomeName = (String) rows[0].get(0);
//        	int phase = (int) rows[0].get(1);
//        	String orientation = (String) rows[0].get(2);
//        	boolean coding = (boolean) rows[0].get(3);
//        	String uniProtId = (String) rows[0].get(4);
//        	snpData = new SNP(phase, orientation, uniProtId);
//        	snpData.setChromosomeName(chromosomeName);
//        	snpData.setCoding(coding);
//        }
//		return snpData;
//	}
	

	
//	public static void readUniprot() {
//		
//		//register the Uniprot to PDB mapping
//	    DataFrame uniprotPDB = sqlContext.read().parquet(userHome+"/data/genevariation/uniprotpdb/20161104");
//	    uniprotPDB.registerTempTable("uniprotPDB");
////	    uniprotPDB.show();
//	}

}
