package exonscorrelation;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.io.PDBDataProvider;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;

public class TestExonsBoundaries {

	private static String path = "/Users/yana/ishaan/";
	private static String filename = "FDR0.gene";
	
	public static List<ExonData> getExons() throws IOException {

		List<ExonData> exons = new ArrayList<ExonData>();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings();
		for (Transcript transcript : transcripts) {
			List<Integer> starts = transcript.getExonStarts();
			List<Integer> ends = transcript.getExonEnds();
			for ( int i=0; i<starts.size();i++ ) {
				ExonData exon = new ExonData();
				exon.setChromosome(transcript.getChromosomeName());
				exon.setGeneName(transcript.getGeneName());
				exon.setGeneBankId(transcript.getGeneBankId());
				exon.setStart(starts.get(i)+1);
				exon.setEnd(ends.get(i));
				exons.add(exon);
			}
		}
		return exons;
	}
	
	public static List<ExonData> getExonsData() {

		String dataPath = path+"FDR0.gene.CDS";
		Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(dataPath);
		Encoder<ExonData> encoder = Encoders.bean(ExonData.class);
		List<ExonData> exons = data.flatMap(new MapToExonData(), encoder).collectAsList();

		Collections.sort(exons, new Comparator<ExonData>() {
			@Override
			public int compare(final ExonData e1, final ExonData e2) {
				return e1.getChromosome().compareTo(e2.getChromosome());
			}
		} );

		return exons;
	}

	public static void mapToGeneBank() {

		Dataset<Row> mp = SaprkUtils.getSparkSession()
				.read().csv(path+"mart_export.txt")
				.filter(t->t.getAs(1)!=null);
		List<ExonData> exons = getExonsData();
		Dataset<Row> df = SaprkUtils.getSparkSession().createDataFrame(exons, ExonData.class).drop("geneBankId");
		
		Dataset<Row> newdf = mp.join(df, df.col("ensemblId").equalTo(mp.col("_c0")), "inner").drop("_c0").withColumnRenamed("_c1", "geneBankId");
		newdf.write().mode(SaveMode.Overwrite).parquet(path+"DATA/exons_data_mapped_to_genebank");
	}
	
	public static void mapToIsoformPositions() {
		
		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"DATA/exons_data_mapped_to_genebank");
		data.persist(StorageLevel.MEMORY_AND_DISK());
		
		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			

		for (String chr : chromosomes) {

			Dataset<Row> map = PDBDataProvider.readHumanChromosomeMapping(chr);
			
			Dataset<Row> df1 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("start").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("cds")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position")).drop(map.col("isoformCorrectedUniprotPos"))
					.drop(map.col("phase")).withColumnRenamed("uniProtPos", "isoformPosStart");
					
			Dataset<Row> df2 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("end").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("cds")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position")).drop(map.col("isoformCorrectedUniprotPos"))
					.drop(map.col("phase")).withColumnRenamed("uniProtPos", "isoformPosEnd");

			Dataset<Row> df = df1.join(df2, 
					df1.col("chromosome").equalTo(df2.col("chromosome"))
					.and(df1.col("geneBankId").equalTo(df2.col("geneBankId")))
					.and(df1.col("ensemblId").equalTo(df2.col("ensemblId")))
					.and(df1.col("isoformNr").equalTo(df2.col("isoformNr")))
					.and(df1.col("start").equalTo(df2.col("start")))
					.and(df1.col("end").equalTo(df2.col("end"))), "inner")
					.drop(df2.col("chromosome")).drop(df2.col("geneBankId")).drop(df2.col("ensemblId"))
					.drop(df2.col("orientation")).drop(df2.col("offset")).drop(df2.col("geneName"))
					.drop(df2.col("start")).drop(df2.col("end")).drop(df2.col("isoformNr")).drop(df2.col("uniProtId"));
			
			df.write().mode(SaveMode.Overwrite).parquet(path+"DATA/exons_isoforms_map/"+chr);
			
		}
	}

	public static void getPeptides() throws Exception {
		
		String chrSet="chr1";
		RNApolymerase polymerase = new RNApolymerase(chrSet);
		
		Map<String, String> map = new HashMap<String, String>();
		
		List<ExonData> exons = getExonsData();
		for (ExonData exon : exons) {

			if ( !chrSet.equals(exon.getChromosome())) {
				polymerase.setChromosome(exon.getChromosome());
				chrSet = exon.getChromosome();
			}

			String transcription = "";
			if ( !exon.getOrientation().equals("+") ) {
				int lenght = ((exon.getEnd()-exon.getOffset()) - exon.getStart())+1;
				int correction = lenght%3;
				lenght = lenght-correction;
				transcription = polymerase.parser.loadFragment((exon.getStart()+correction)-1, lenght);
				transcription = new StringBuilder(transcription).reverse().toString();
				DNASequence dna = new DNASequence(transcription);
				SequenceView<NucleotideCompound> compliment = dna.getComplement();
				transcription = compliment.getSequenceAsString();
			}
			else {
				int lenght = (exon.getEnd() - (exon.getStart()+exon.getOffset()))+1;
				int correction = lenght%3;
				lenght = lenght-correction;
				transcription = polymerase.parser.loadFragment((exon.getStart()+exon.getOffset())-1, lenght);
			}

			String peptide = Ribosome.getProteinSequence(transcription);
			map.put(exon.getGeneBankId(), peptide);
		}
	}
	
	public static void getExonsDisorderPrediction() throws Exception {

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			

		for (String chr : chromosomes) {
			
			System.out.println(chr);
			
			Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"DATA/exons_isoforms_map/"+chr);
			
			Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToProteinDisorder(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToDisorderString(), Encoders.STRING()).collectAsList();
			FileWriter writer = new FileWriter(path+"DATA/disorder_prediction_"+chr+".csv"); 
			for(String str: features) {
			  writer.write(str+"\n");
			}
			writer.close();	
		}
	}

	public static void mappingToGeneBankId() throws IOException {

		// get the experimental exons data
		String dataPath = path+filename;
		Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(dataPath);
		data.createOrReplaceTempView("t1");

		List<ExonData> exons = getExons();
		Dataset<Row> exonsDf = SaprkUtils.getSparkSession().createDataFrame(exons, ExonData.class);
		exonsDf.createOrReplaceTempView("t2");

		Dataset<Row> coding = SaprkUtils.getSparkSession().read().csv(path+"coding_genes.list");
		coding.createOrReplaceTempView("t4");

		Dataset<Row> mappingDf = SaprkUtils.getSparkSession().sql("select t2.chromosome, t2.geneName, t2.geneBankId,"
				+ "t2.start, t2.end, t1._c3 as orientation from t1 inner join t2 on (t1._c0 = t2.chromosome and "
				+ "t1._c1 = t2.start and t1._c2 = t2.end) inner join t4 on (t2.geneName = t4._c0)");
		mappingDf.createOrReplaceTempView("t3");
		mappingDf.write().mode(SaveMode.Overwrite).parquet(path+"mappingToGeneBankId");
	}

	public static void mappingToUniprotPos() throws IOException {

		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"mappingToGeneBankId");
		data.createOrReplaceTempView("tbl");

		//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
		//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			
		String[] chromosomes = {"chr11"};

		for (String chr : chromosomes) {

			Dataset<Row> map = PDBDataProvider.readHumanChromosomeMapping(chr);
			map.createOrReplaceTempView("map");

			Dataset<Row> df1 = SaprkUtils.getSparkSession().sql("select map.chromosome, map.geneBankId, map.geneSymbol as geneName, tbl.start, tbl.end, "
					+ "map.uniProtPos as isoformStart, map.isoformNr, "
					+ "map.uniProtId, map.orientation from map inner join tbl on (map.chromosome=tbl.chromosome and map.geneBankId=tbl.geneBankId and"
					+ " tbl.start=map.position)").distinct();
			df1.createOrReplaceTempView("m1");

			Dataset<Row> df2 = SaprkUtils.getSparkSession().sql("select m1.chromosome, m1.geneName, m1.geneBankId, m1.orientation, m1.start, m1.end, "
					+ "m1.uniProtId, m1.isoformNr, m1.isoformStart, map.uniProtPos as isoformEnd from m1 inner join map "
					+ "on (m1.chromosome=map.chromosome and m1.geneBankId=map.geneBankId and m1.isoformNr=map.isoformNr and map.position=m1.end)");
			df2.orderBy("geneName", "start", "isoformNr").show();

		}
	}

	public static void getExonsHydropathy() throws Exception {

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			

		for (String chr : chromosomes) {
			
			System.out.println(chr);
			
			Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"DATA/exons_isoforms_map/"+chr);
			
			Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToProteinHydropathy(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToHydropathyString(), Encoders.STRING()).collectAsList();
			FileWriter writer = new FileWriter(path+"DATA/hydropathy_calculation_"+chr+".csv"); 
			for(String str: features) {
			  writer.write(str+"\n");
			}
			writer.close();
		}
	}
             	
	public static void getExonsAACharges() throws Exception {

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			

		for (String chr : chromosomes) {
			
			System.out.println(chr);
			
			Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"DATA/exons_isoforms_map/"+chr);
			
			Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToAACharges(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToChargesString(), Encoders.STRING()).collectAsList();
			FileWriter writer = new FileWriter(path+"DATA/amino_acid_charges_"+chr+".csv"); 
			for(String str: features) {
			  writer.write(str+"\n");
			}
			writer.close();
		}
	}
	
	public static void getExonsAAPolarity() throws Exception {

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};			

		for (String chr : chromosomes) {
			
			System.out.println(chr);
			
			Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(path+"DATA/exons_isoforms_map/"+chr);
			
			Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToAAPolarity(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToPolarityString(), Encoders.STRING()).collectAsList();
			FileWriter writer = new FileWriter(path+"DATA/amino_acid_polarity_"+chr+".csv"); 
			for(String str: features) {
			  writer.write(str+"\n");
			}
			writer.close();
		}
	}
	
	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();

		//getExonsDisorderPrediction();
		//getExonsHydropathy();
		//getExonsAACharges();
		getExonsAAPolarity();
		
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
