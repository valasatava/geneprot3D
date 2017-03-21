package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.utils.SaprkUtils;

public class PDBDataProvider extends DataProvider {
	
	private final static String dfGenevariationPath = getProjecthome() + "parquet/hg38/";
	private final static String dfUniprotpdbPath = getProjecthome() + "parquet/uniprotpdb/20161104/";
	

	public static Dataset<Row> readHumanChromosomeMapping(String chr) {
        Dataset<Row> chrMapping = SaprkUtils.getSparkSession().read().parquet(dfGenevariationPath+chr);
        return chrMapping;
	}
	
	public static Dataset<Row> readPdbUniprotMapping() {
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(dfUniprotpdbPath);
		return mapping;
	}
	
	public static void main(String[] args) {
		
		Dataset<Row> map = readHumanChromosomeMapping("chr18");
		map.createOrReplaceTempView("map");
		
		Dataset<Row> df = SaprkUtils.getSparkSession().sql("select * from map where (geneSymbol='CEP192' and (position=13071039 or position=13071212)) order by position, isoformNr");
		df.show();
		
//		String path = "/Users/yana/ishaan/FDR0.gene";
//		Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(path);
//		data.createOrReplaceTempView("t");
//		data.show();
//		
//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};		
//		for (String chr : chromosomes) {
//			Dataset<Row> map = readHumanChromosomeMapping(chr);
//			map.createOrReplaceTempView("map");
//			
//			Dataset<Row> df = SaprkUtils.getSparkSession().sql("select * from map inner join t "
//					+ "on (t._c0=map.chromosome and t._c4=map.geneSymbol)");
//			df.write().mode(SaveMode.Overwrite).csv("/Users/yana/ishaan/exons_gene.map.to.uniprot/"+chr);	
//		}
	}
}
