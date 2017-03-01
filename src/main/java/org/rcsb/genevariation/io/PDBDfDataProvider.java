package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

public class PDBDfDataProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String dfGenevariationPath = userHome + "/data/genevariation/hg38/";
	private final static String dfUniprotpdbPath = userHome + "/data/genevariation/uniprotpdb/20161104";
	

	public static Dataset<Row> readHumanChromosomeMapping(String chrN) {	
		String chrname = "chr"+chrN;
        Dataset<Row> chr = SaprkUtils.getSparkSession().read().parquet(dfGenevariationPath+chrname);
        return chr.filter("chromosome='"+chrname+"'").orderBy("position");
	}
	
	public static Dataset<Row> readPdbUniprotMapping() {
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(dfUniprotpdbPath);
		return mapping;
	}
	
	public static void main(String[] args) {
		Dataset<Row> map = readHumanChromosomeMapping("21");
		//Dataset<Row> map = readPdbUniprotMapping();
		map.show();
	}
}
