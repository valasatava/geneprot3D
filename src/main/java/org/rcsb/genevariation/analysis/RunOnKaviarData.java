package org.rcsb.genevariation.analysis;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.datastructures.VcfContainer;
import org.rcsb.genevariation.io.DataProvider;
import org.rcsb.genevariation.io.PDBDataProvider;
import org.rcsb.genevariation.mappers.FilterCodingRegion;
import org.rcsb.genevariation.mappers.FilterSNPs;
import org.rcsb.genevariation.mappers.MapToMutation;
import org.rcsb.genevariation.mappers.MapToVcfContainer;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.rcsb.genevariation.utils.VariationUtils;

import java.io.IOException;
import java.util.List;

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
				.load(filepathVCF)
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

	        Dataset<Row> mapping = SaprkUtils.getSparkSession().sql("select hgmapping.geneSymbol, kaviar.dbSnpID, hgmapping.chromosome, hgmapping.position, hgmapping.inCoding, "
	        		+ "hgmapping.uniProtId, hgmapping.uniProtPos, kaviar.original, kaviar.variant, hgmapping.orientation from kaviar " +
	                "inner join hgmapping on ( hgmapping.chromosome = kaviar.chromosome and hgmapping.position = kaviar.position )");
	        mapping.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping/"+chr);
		}
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}

	public static void writeKaviarOnHumanGenome() throws AnalysisException {

		Dataset<Row> all = null;
		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};
		for (String chr : chromosomes) {

			String filepath = DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping/"+chr;
			Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath);
			mapping.createOrReplaceTempView("mapping");

			Dataset<Row> coding = SaprkUtils.getSparkSession().sql("select * from mapping where inCoding=true");

			if (all == null ) {
				all = coding;
			}
			else {
				all = all.union(coding);
			}
		}
		all.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping-coding.parquet/");
	}

	public static void mapKaviarToMutations() throws AnalysisException, Exception {

		long start = System.nanoTime();

		JavaSparkContext sc = SaprkUtils.getSparkContext();
		List<GeneChromosomePosition> transcripts = GenePredictionsParser.getGeneChromosomePositions();
		Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast = sc.broadcast(transcripts);

		String filepath = DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping-coding.parquet/";
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath)
				.drop("inCoding")
				.filter(new FilterFunction<Row>() {
					@Override
					public boolean call(Row row) throws Exception {
						String wildtype = row.get(6).toString();
						String mutation = row.get(7).toString();
						if (VariationUtils.checkType(wildtype, mutation).compareTo(VariantType.SNP) == 0) {
							return true;
						}
						return false;
					}
				});
		mapping.createOrReplaceTempView("mapping");

		Encoder<Mutation> mutationEncoder = Encoders.bean(Mutation.class);
		Dataset<Mutation> mutations = mapping.flatMap(new MapToMutation(transcriptsBroadcast), mutationEncoder);
		mutations.createOrReplaceTempView("mutations");

		Dataset<Row> df = SaprkUtils.getSparkSession().sql("select * from mapping inner join mutations on (mapping.chromosome = mutations.chromosomeName and mapping.position = mutations.position)");
		df.show();

		//.write().mode(SaveMode.Overwrite).parquet(DataProvider.getProjecthome() + "parquet/Kaviar-mutations.parquet");

		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}

	public static void readKaviarOnHumanGenome() throws AnalysisException {

		String filepath = DataProvider.getProjecthome() + "parquet/Kaviar-hg-mapping-coding.parquet/";
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath);
		mapping.createOrReplaceTempView("kaviar");

		mapping.show();
		System.out.println(mapping.count());
	}

	public static void main(String[] args) throws Exception {
		//writeKaviar();
		//readKaviar();
		//mapKaviarOnHumanGenome();
		//readKaviarOnHumanGenome();
		mapKaviarToMutations();
	}
}
