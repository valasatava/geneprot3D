package org.rcsb.genevariation.io;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.pharmgkb.parser.vcf.VcfParser;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.Deletion;
import org.rcsb.genevariation.datastructures.Insertion;
import org.rcsb.genevariation.datastructures.Monomorphism;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.datastructures.SNP;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.filters.DataProviderFilterSNP;
import org.rcsb.genevariation.filters.IDataProviderFilter;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.rcsb.genevariation.utils.VariationUtils;

import com.google.common.collect.ListMultimap;

/**
 * This class provides methods to retrieve variation data from files.
 * 
 * @author Yana Valasatava
 */
public class VariantsDataProvider extends DataProvider {
	
	private static List<Variant> variants;
	private final static String variationDataPath = getProjecthome() +  "common_and_clinical_20170130.vcf";
	
	public VariantsDataProvider() {
		variants = new ArrayList<Variant>();
	}
	
	private static void addVariant(Variant variant) {
		variants.add(variant);
	}
	
	public void readVariantsFromVCF() throws IOException {
		readVariantsFromVCF(Paths.get(variationDataPath));
	}
	
	/** 
	 * The method reads VCF file and builds a library of variations.
	 * 
	 * @param File path to VCF file as Path.
	 */
	public void readVariantsFromVCF(Path filepath) throws IOException {

		VcfParser parser = new VcfParser.Builder().fromFile(filepath).parseWith((metadata, position, sampleData) -> {
			
			String chromosome = "chr"+position.getChromosome();
			long pos = position.getPosition();
			String ref = position.getRef();
			List<String> alts = position.getAltBases();

			for (String alt : alts) {

				Variant variant = null;
				VariantType type = VariationUtils.checkType(ref, alt);

				switch (type) {
				case SNP:
					variant = new SNP(chromosome, pos, type);
					// RV metadata tells if the variation is called on the reverse gene
					ListMultimap<String, String> inf = position.getInfo();
					if (inf.asMap().containsKey("RV")) {
						ref = VariationUtils.reverseComplimentaryBase(ref);
						alt = VariationUtils.reverseComplimentaryBase(alt);
					}
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
				variant.setVariation(ref, alt);
				addVariant(variant);
			}
		}).build();
		parser.parse();
	}
	
	/**
	 * Gets all variation data.
	 * 
	 */
	public Iterator<Variant> getAllVariants() {
		return variants.iterator();
	}
	
	/**
	 * Gets variation data by applying the given filter.
	 * 
	 * @param dataFilter - an implementation class of IDataProviderFilter
	 * @return An iterator over a collection of Variants
	 */
	public Iterator<Variant> getVariantsByFilter(IDataProviderFilter dataFilter) {
		
		List<Variant> filteredVariants = new ArrayList<Variant>();
		for (Variant variant : variants) {
			if ( dataFilter.filter(variant) ) {
				filteredVariants.add(variant);
			}
		}
		return filteredVariants.iterator();
	}

	public void setVariants(List<Variant> vars) {
		variants = new ArrayList<Variant>();
		for (Variant variant : vars) {
			variants.add(variant);
		}
	}
	
	public void setVariants(Iterator<Variant> vars) {
		variants = new ArrayList<Variant>();
		while (vars.hasNext()) {
			Variant variant = vars.next();
			variants.add(variant);
		}
	}
	
	public List<Mutation> getMutations(IDataProviderFilter dataFilter) throws Exception {
		
		List<Mutation> mutations = new ArrayList<>();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings();
		
		// Filter SNPs
		setVariants(getVariantsByFilter(dataFilter));
		Iterator<Variant> variations = getAllVariants();
		
		String chrName = "";
		RNApolymerase polymerase = new RNApolymerase();
		while (variations.hasNext()) {
			
			Variant variant = variations.next();
			String chrom = variant.getChromosomeName();
			if (!chrom.equals(chrName)) {
				polymerase.setChromosome(chrom);
				chrName = chrom;
			}
			
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
					mutation.setRefAminoAcid(Ribosome.getCodingAminoAcid(codon));
					mutation.setMutAminoAcid(Ribosome.getCodingAminoAcid(mutCodon));
					mutations.add(mutation);
				}
			}	
		}	
		return mutations;
	}
	
	public List<Mutation> getSNPMutations() throws Exception {
		
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();
		List<Mutation> mutations = getMutations(dataFilterVar);
		return mutations;
	}
	
	public void createVariationDataFrame(List<Mutation> mutations) {
		
		Dataset<Row> mydf = SaprkUtils.getSparkSession().createDataFrame(mutations, Mutation.class);
		mydf.write().mode(SaveMode.Overwrite).parquet(getProjecthome() + "variations.parquet");
	}
	
	public Dataset<Row> getMissenseVariationDF(String path) {
		
        Dataset<Row> mutations = SaprkUtils.getSparkSession().read().parquet(path);
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
		vdp.createVariationDataFrame(mutations);
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
