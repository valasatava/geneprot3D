package exonscorrelation;

import exonscorrelation.mappers.*;
import exonscorrelation.utils.CommonUtils;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.datastructures.ExonSerializable;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.io.PDBDataProvider;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.IOException;
import java.util.*;

public class AnalyzeExonsProteinFeatures {

	private static String path = "/Users/yana/ishaan/";

	public static void mapExonsToIsoformPositions(String exonsdatapath, String exonsuniprotpath) {

		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsdatapath);
		data.persist(StorageLevel.MEMORY_AND_DISK());

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

		for (String chr : chromosomes) {

			Dataset<Row> map = PDBDataProvider.readHumanChromosomeMapping(chr);

			Dataset<Row> df1 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("start").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position")).drop(map.col("uniProtCanonicalPos"))
					.drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosStart");

			Dataset<Row> df2 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("end").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position")).drop(map.col("uniProtCanonicalPos"))
					.drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosEnd");

			Dataset<Row> df = df1.join(df2,
					df1.col("chromosome").equalTo(df2.col("chromosome"))
					.and(df1.col("geneBankId").equalTo(df2.col("geneBankId")))
					.and(df1.col("ensemblId").equalTo(df2.col("ensemblId")))
					.and(df1.col("isoformIndex").equalTo(df2.col("isoformIndex")))
					.and(df1.col("start").equalTo(df2.col("start")))
					.and(df1.col("end").equalTo(df2.col("end"))), "inner")
					.drop(df2.col("chromosome")).drop(df2.col("geneBankId")).drop(df2.col("ensemblId"))
					.drop(df2.col("orientation")).drop(df2.col("offset")).drop(df2.col("geneName"))
					.drop(df2.col("start")).drop(df2.col("end")).drop(df2.col("isoformIndex")).drop(df2.col("uniProtId"));

			df.write().mode(SaveMode.Overwrite).parquet(exonsuniprotpath+"/"+chr);
		}
	}

	public static void mapToPDBPositions(String uniprotmapping, String pdbmapping ) {

		Dataset<Row> mapToPdb = PDBDataProvider.readPdbUniprotMapping();

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

		for (String chr : chromosomes) {

			Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);

		}

	}

	public static void getPeptides() throws Exception {

		String chrSet="chr1";
		RNApolymerase polymerase = new RNApolymerase(chrSet);

		Map<String, String> map = new HashMap<String, String>();

		List<ExonSerializable> exons = getExonsData("");
		for (ExonSerializable exon : exons) {

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
				int length = (exon.getEnd() - (exon.getStart()+exon.getOffset()))+1;
				int correction = length%3;
				length = length-correction;
				transcription = polymerase.parser.loadFragment((exon.getStart()+exon.getOffset())-1, length);
			}

			String peptide = Ribosome.getProteinSequence(transcription);
			map.put(exon.getGeneBankId(), peptide);
		}
	}




	public static void getExonsDisorderPrediction(String chr, Encoder<ExonProteinFeatures> encoder, Dataset<Row> data) throws Exception {

		Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToProteinDisorder(), encoder)
					.filter(t->t!=null);
		List<String> features = featuresDf.map(new MapToDisorderString(), Encoders.STRING()).collectAsList();

		String fpath = path + "DATA/disorder_prediction_" + chr + ".csv";
		CommonUtils.writeListOfStringsInFile(features, fpath);
	}

	public static void getExonsHydropathy(String chr, Encoder<ExonProteinFeatures> encoder, Dataset<Row> data) throws Exception {

			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToProteinHydropathy(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToHydropathyString(), Encoders.STRING()).collectAsList();

			String fpath = path + "DATA/hydropathy_calculation_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(features, fpath);
	}

	public static void getExonsAACharges(String chr, Encoder<ExonProteinFeatures> encoder, Dataset<Row> data) throws Exception {

			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToAACharges(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToChargesString(), Encoders.STRING()).collectAsList();

			String fpath = path + "DATA/amino_acid_charges_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(features, fpath);
	}

	public static void getExonsAAPolarity(String chr, Encoder<ExonProteinFeatures> encoder, Dataset<Row> data) throws Exception {

			Dataset<ExonProteinFeatures> featuresDf = data.map(new MapToAAPolarity(), encoder)
					.filter(t->t!=null);
			List<String> features = featuresDf.map(new MapToPolarityString(), Encoders.STRING()).collectAsList();

			String fpath = path + "DATA/amino_acid_polarity_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(features, fpath);
	}

	public static void runAll(String exonsuniprotpath) throws Exception {

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

		for (String chr : chromosomes) {

			System.out.println("Processing chromosome: "+chr);

			Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
			Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsuniprotpath+"/"+chr);
			Dataset<ExonProteinFeatures> featuresDF = data.map(new MapToProteinFeatures(), encoder)
					.filter(t->t!=null);
			featuresDF.persist();

			List<String> disorder = featuresDF.map(new MapToDisorderString(), Encoders.STRING()).collectAsList();
			String disorderfpath = path + "DATA/disorder_prediction_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(disorder, disorderfpath);

			List<String> hydropathy = featuresDF.map(new MapToHydropathyString(), Encoders.STRING()).collectAsList();
			String hydropathyfpath = path + "DATA/hydropathy_calculation_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(hydropathy, hydropathyfpath);

			List<String> charges = featuresDF.map(new MapToChargesString(), Encoders.STRING()).collectAsList();
			String chargesfpath = path + "DATA/amino_acid_charges_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(charges, chargesfpath);

			List<String> polarity = featuresDF.map(new MapToPolarityString(), Encoders.STRING()).collectAsList();
			String polarityfpath = path + "DATA/amino_acid_polarity_" + chr + ".csv";
			CommonUtils.writeListOfStringsInFile(polarity, polarityfpath);
		}
	}




	public static List<ExonSerializable> getUCSCExons() throws IOException {

		List<ExonSerializable> exons = new ArrayList<ExonSerializable>();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings();
		for (Transcript transcript : transcripts) {
			List<Integer> starts = transcript.getExonStarts();
			List<Integer> ends = transcript.getExonEnds();
			for ( int i=0; i<starts.size();i++ ) {
				ExonSerializable exon = new ExonSerializable();
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

	public static void sortExons(List<ExonSerializable> exons) {
		//sorting the exons based on the chromosome name
		Collections.sort(exons, new Comparator<ExonSerializable>() {
			@Override
			public int compare(final ExonSerializable e1, final ExonSerializable e2) {
				return e1.getChromosome().compareTo(e2.getChromosome());
			}
		} );
	}

	public static List<ExonSerializable> getExonsData(String dataPath) {

		Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(dataPath);

		Encoder<ExonSerializable> encoder = Encoders.bean(ExonSerializable.class);
		List<ExonSerializable> exons = data.map(new MapToExonSerializable(), encoder).collectAsList();

		return exons;
	}

	public static Dataset<Row> getGeneBankToEnsembleMapping() {

		// Ensembl to gene bank id mapping
		Dataset<Row> mp = SaprkUtils.getSparkSession()
				.read().csv(path+"MAPS/mart_export.txt")
				.filter(t->t.getAs(1)!=null)
				.withColumnRenamed("_c0", "ensemblId")
				.withColumnRenamed("_c1", "geneBankId");
		return mp;
	}

	public static void mapExonsToGeneBank(List<ExonSerializable> exons, String path) throws IOException {

		Dataset<Row> exonsDF = SaprkUtils.getSparkSession().createDataFrame(exons, ExonSerializable.class);
		exonsDF.createOrReplaceTempView("exons");

		Dataset<Row> geneBankMapping = getGeneBankToEnsembleMapping();
		geneBankMapping.createOrReplaceTempView("genebank");

		Dataset<Row> mappingDF = SaprkUtils.getSparkSession().sql("select exons.chromosome, exons.geneName, exons.ensemblId, genebank.geneBankId, "
				+ "exons.orientation, exons.offset, exons.start, exons.end from exons inner join genebank on (exons.ensemblId = genebank.ensemblId)");
		mappingDF.write().mode(SaveMode.Overwrite).parquet(path);
	}

	public static void runGeneBankMapping() throws Exception {

		String datapath = path+"EXONS_DATA/gencode.v24.CDS.protein_coding.gtf";
		List<ExonSerializable> exons = getExonsData(datapath);

		String mappingpath = path+"MAPS/gencode.v24.CDS.protein_coding.gene_bank_mapping";
		mapExonsToGeneBank(exons, mappingpath);
	}


	public static void test() {

		String exonsuniprotpath = path+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping/chr14";

		Encoder<ExonProteinFeatures> encoder = Encoders.bean(ExonProteinFeatures.class);
		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsuniprotpath);

		Dataset<ExonProteinFeatures> featuresDF = data.map(new MapToProteinDisorder(), encoder)
				.filter(t->t!=null);
		featuresDF.count();
	}

	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();

//		String exonsdatapath = path+"MAPS/gencode.v24.CDS.protein_coding.gene_bank_mapping";
		String exonsuniprotpath = path+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping";

//		Dataset<Row> mp = SaprkUtils.getSparkSession()
//				.read().parquet(exonsuniprotpath+"/chr21");
//		mp.show();

//		runAll(exonsuniprotpath);

		test();

		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
