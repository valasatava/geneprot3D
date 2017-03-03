package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

public class PDBDataProvider extends DataProvider {
	
	private final static String dfGenevariationPath = getProjecthome() + "hg38/";
	private final static String dfUniprotpdbPath = getProjecthome() + "uniprotpdb/20161104/";
	

	public static Dataset<Row> readHumanChromosomeMapping(String chr) {
        Dataset<Row> chrMapping = SaprkUtils.getSparkSession().read().parquet(dfGenevariationPath+chr);
        return chrMapping;
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
