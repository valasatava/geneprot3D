package org.rcsb.genevariation.analysis;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.rcsb.genevariation.datastructures.VcfContainer;
import org.rcsb.genevariation.io.DataProvider;
import org.rcsb.genevariation.io.PDBDataProvider;
import org.rcsb.genevariation.mappers.FilterCodingRegion;
import org.rcsb.genevariation.mappers.FilterSNPs;
import org.rcsb.genevariation.mappers.MapToVcfContainer;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;

public class RunOnKaviarData {
	
	private static String filepathVCF = DataProvider.getProjecthome() + "vcfs/Kaviar-160204-Public-hg38-trim.vcf";
	private static String filepathParquet = DataProvider.getProjecthome() + "parquet/Kaviar-database.parquet";
	
	public static void run() throws Exception {
		
		long start = System.nanoTime();

		JavaSparkContext sc = SaprkUtils.getSparkContext();
		List<GeneChromosomePosition> transcripts = GenePredictionsParser.getGeneChromosomePositions();
		Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast = sc.broadcast(transcripts);

		Encoder<VcfContainer> vcfContainerEncoder = Encoders.bean(VcfContainer.class);
		
		SaprkUtils.getSparkSession().read()
				.format("com.databricks.spark.csv")
				.option("header", "false")
				.option("delimiter", "\t")
				.option("comment", "#")
				.load(filepathVCF)
				.flatMap(new MapToVcfContainer(), vcfContainerEncoder)
				.filter(new FilterCodingRegion(transcriptsBroadcast))
				.filter(new FilterSNPs())
				//.repartition(500)
				.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/coding-snps-Kaviar.parquet");
	
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
	
	public static void writeKaviar() throws IOException {
		
		long start = System.nanoTime();
		
		Encoder<VcfContainer> vcfContainerEncoder = Encoders.bean(VcfContainer.class);
		SaprkUtils.getSparkSession().read()
				.format("com.databricks.spark.csv")
				.option("header", "false")
				.option("delimiter", "\t")
				.option("comment", "#")
				.load(filepathVCF)//.repartition(250)
				.flatMap(new MapToVcfContainer(), vcfContainerEncoder)
				.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/Kaviar-database.parquet");
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
		
	public static void readKaviar() {
		
		long start = System.nanoTime();
		Dataset<Row> df = SaprkUtils.getSparkSession().read().parquet(filepathParquet);
        df.createOrReplaceTempView("Kaviar");
        df.show();
        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
	
	public static void mapKaviarOnHumanGenome() {
		
		long start = System.nanoTime();
		
		Dataset<Row> df = SaprkUtils.getSparkSession().read().parquet(filepathParquet);
        df.createOrReplaceTempView("kaviar");
        df.persist();
        
        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};		
		for (String chr : chromosomes) {
			
			System.out.println("getting the data for the chromosome "+ chr);
			Dataset<Row> chromMapping = PDBDataProvider.readHumanChromosomeMapping(chr);
			chromMapping.createOrReplaceTempView("hgmapping");

	        Dataset<Row> mapping = SaprkUtils.getSparkSession().sql("select hgmapping.geneSymbol, kaviar.dbSnpID, hgmapping.chromosome, hgmapping.position, "
	        		+ "hgmapping.uniProtId, hgmapping.uniProtPos, kaviar.original, kaviar.variant from kaviar " +
	                "inner join hgmapping on ( hgmapping.chromosome = kaviar.chromosome and hgmapping.position = kaviar.position ) ");
	        mapping.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping/"+chr);
		}
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
	
	public static void main(String[] args) throws Exception {
		//writeKaviar();
		//readKaviar();
		mapKaviarOnHumanGenome();
	}
}
