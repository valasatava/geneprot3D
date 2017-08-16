package org.rcsb.geneprot.genevariation.io;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.geneprot.genes.constants.StrandOrientation;
import org.rcsb.geneprot.genes.parsers.GenePredictionsParser;
import org.rcsb.geneprot.genevariation.constants.VariantType;
import org.rcsb.geneprot.genevariation.datastructures.Insertion;
import org.rcsb.geneprot.genevariation.filters.IVariantDataFilter;
import org.pharmgkb.parser.vcf.VcfParser;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.genevariation.datastructures.Deletion;
import org.rcsb.geneprot.genevariation.datastructures.Monomorphism;
import org.rcsb.geneprot.genevariation.datastructures.Mutation;
import org.rcsb.geneprot.genevariation.datastructures.SNP;
import org.rcsb.geneprot.genes.datastructures.Transcript;
import org.rcsb.geneprot.genevariation.datastructures.VariantInterface;
import org.rcsb.geneprot.genes.expression.RNApolymerase;
import org.rcsb.geneprot.genes.expression.Ribosome;
import org.rcsb.geneprot.genevariation.filters.VariantDataFilterSNP;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genevariation.utils.VariationUtils;

import com.google.common.collect.ListMultimap;

/**
 * This class provides methods to retrieve variation data from files.
 *
 * @author Yana Valasatava
 */
public class VariantsDataProvider extends DataLocationProvider {

	private static List<VariantInterface> variants;
	private final static String variationDataPath = getDataHome() +  "common_and_clinical_20170130.vcf";

	public VariantsDataProvider() {
		variants = new ArrayList<VariantInterface>();
	}

	private static void addVariant(VariantInterface variant) {
		variants.add(variant);
	}

	public static VariantInterface createVariant(String chromosome, long pos, String ref, String alt, boolean reverse) {

		VariantInterface variant = null;
		VariantType type = VariationUtils.checkType(ref, alt);

		switch (type) {
		case SNP:
			variant = new SNP(chromosome, pos, type);
			break;

		case MONOMORPHIC:
			variant = new Monomorphism(chromosome, pos, type);
			break;

		case INSERTION:
			variant = new Insertion(chromosome, pos, type);
			break;

		case DELETION:
			variant = new Deletion(chromosome, pos, type);
			break;

		default:
			System.out.println("NEW!");
			break;
		}
		variant.setReverse(reverse);
		variant.setVariation(ref, alt);
		return variant;
	}


	public void readVariantsFromVCFWithSpark(String filepath) {

		Dataset<Row> df = SparkUtils.getSparkSession().read()
				.format("com.databricks.spark.csv")
				.option("header", "false")
				.option("delimiter", "\t")
				.option("comment", "#")
				.load(filepath);//.flatMap(new MapToSNPs(), encoder);

        for (Iterator<Row> iter = df.collectAsList().iterator(); iter.hasNext(); ) {

        	Row row = iter.next();

            String chromosome =row.getString(0);
            long pos = Long.valueOf(row.getString(1));

            String ref = row.getString(3);
			List<String> alts = Arrays.asList(row.getString(4).split(","));

			boolean reverse = false;
            if (row.getString(7).contains(";RV;")) {
            	reverse = true;
            }

			for (String alt : alts) {
				addVariant(createVariant(chromosome, pos, ref, alt, reverse));
			}
        }
	}

	public void readVariantsFromVCF() throws IOException {
		readVariantsFromVCFWithParser(Paths.get(variationDataPath));

	}

	public void readVariantsFromVCFWithParser(String filepath) throws IOException {
		readVariantsFromVCFWithParser(Paths.get(filepath));
	}

	/**
	 * The method reads VCF file and builds a library of variations.
	 *
	 * @param File path to VCF file as Path.
	 */
	public void readVariantsFromVCFWithParser(Path filepath) throws IOException {

		VcfParser parser = new VcfParser.Builder().fromFile(filepath).parseWith((metadata, position, sampleData) -> {

			String chromosome = "chr"+position.getChromosome();
			long pos = position.getPosition();
			String ref = position.getRef();
			List<String> alts = position.getAltBases();

			ListMultimap<String, String> inf = position.getInfo();
			boolean reverse = false;
			if (inf.asMap().containsKey("RV")) {
				reverse = true;
			}

			for (String alt : alts) {
				System.out.println(chromosome+" "+pos);
				addVariant(createVariant(chromosome, pos, ref, alt, reverse));
			}
		}).build();
		parser.parse();
	}

	/**
	 * Gets all variation data.
	 *
	 */
	public Iterator<VariantInterface> getAllVariants() {
		return variants.iterator();
	}

	/**
	 * Gets variation data by applying the given filter.
	 *
	 * @param dataFilter - an implementation class of IVariantDataFilter
	 * @return An iterator over a collection of Variants
	 */
	public Iterator<VariantInterface> getVariantsByFilter(IVariantDataFilter dataFilter) {

		List<VariantInterface> filteredVariants = new ArrayList<VariantInterface>();
		for (VariantInterface variant : variants) {
			if ( dataFilter.filter(variant) ) {
				filteredVariants.add(variant);
			}
		}
		return filteredVariants.iterator();
	}

	public void setVariants(List<VariantInterface> vars) {
		variants = new ArrayList<VariantInterface>();
		for (VariantInterface variant : vars) {
			variants.add(variant);
		}
	}

	public void setVariants(Iterator<VariantInterface> vars) {
		variants = new ArrayList<VariantInterface>();
		while (vars.hasNext()) {
			VariantInterface variant = vars.next();
			variants.add(variant);
		}
	}

	public List<Mutation> getMutations(IVariantDataFilter dataFilter) throws Exception {

		List<Mutation> mutations = new ArrayList<>();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings();

		// Filter SNPs
		setVariants(getVariantsByFilter(dataFilter));
		Iterator<VariantInterface> variations = getAllVariants();

		String chrName = "";
		RNApolymerase polymerase = new RNApolymerase();
		while (variations.hasNext()) {

			VariantInterface variant = variations.next();

			for (Transcript transcript : transcripts) {

				if ( ( variant.getPosition() >= transcript.getCodingStart() ) && (variant.getPosition() <= transcript.getCodingEnd()) ) {

					int mRNApos = polymerase.getmRNAPositionForGeneticCoordinate((int) variant.getPosition(), transcript);
					if (mRNApos == -1)
						continue;

					String codingSequence = polymerase.getCodingSequence(transcript);
					String codon = polymerase.getCodon(mRNApos, codingSequence);

					String mutBase = variant.getAltBase();
					String mutCodon="";
					if (transcript.getOrientation().equals(StrandOrientation.FORWARD)) { mutCodon = VariationUtils.mutateCodonForward(mRNApos, codon, mutBase); }
					else { mutCodon = VariationUtils.mutateCodonReverse(mRNApos, codon, mutBase); }

					Mutation mutation = new Mutation();
					mutation.setChromosomeName(chrName);
					mutation.setGeneBankId(transcript.getGeneBankId());
					mutation.setPosition(variant.getPosition());
					mutation.setRefAminoAcid(Ribosome.getProteinSequence(codon));
					mutation.setMutAminoAcid(Ribosome.getProteinSequence(mutCodon));
					mutations.add(mutation);
				}
			}
		}
		return mutations;
	}

	public List<Mutation> getSNPMutations() throws Exception {

		IVariantDataFilter dataFilterVar = new VariantDataFilterSNP();
		List<Mutation> mutations = getMutations(dataFilterVar);
		return mutations;
	}

	public void createVariationDataFrame(List<Mutation> mutations, String filename) {

		Dataset<Row> mydf = SparkUtils.getSparkSession().createDataFrame(mutations, Mutation.class);
		mydf.write().mode(SaveMode.Overwrite).parquet(getDataHome() + filename);
	}

	public Dataset<Row> getMissenseVariationDF(String path) {

        Dataset<Row> mutations = SparkUtils.getSparkSession().read().parquet(path);
        mutations.createOrReplaceTempView("mutations");

        Dataset<Row> missense = mutations.filter(mutations.col("refAminoAcid").notEqual(mutations.col("mutAminoAcid")));
        missense.createOrReplaceTempView("missense");

        return missense;
	}

	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();
		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF();
		List<Mutation> mutations = vdp.getSNPMutations();
		vdp.createVariationDataFrame(mutations, "variations.parquet");
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
