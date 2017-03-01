package org.rcsb.genevariation.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pharmgkb.parser.vcf.VcfParser;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.Deletion;
import org.rcsb.genevariation.datastructures.Insertion;
import org.rcsb.genevariation.datastructures.Monomorphism;
import org.rcsb.genevariation.datastructures.SNP;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.filters.IDataProviderFilter;
import org.rcsb.genevariation.utils.VariationUtils;

import com.google.common.collect.ListMultimap;

/**
 * This class provides methods to retrieve variation data from files.
 * 
 * @author Yana Valasatava
 */
public class VariantsDataProvider {
	
	private List<Variant> variants;
	
	public VariantsDataProvider() {
		variants = new ArrayList<Variant>();
	}
	
	private void addVariant(Variant variant) {
		this.variants.add(variant);
	}
	
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

	public void setVariants(List<Variant> variants) {
		this.variants = new ArrayList<Variant>();
		for (Variant variant : variants) {
			this.variants.add(variant);
		}
	}
	
	public void setVariants(Iterator<Variant> variants) {
		this.variants = new ArrayList<Variant>();
		while (variants.hasNext()) {
			Variant variant = variants.next();
			this.variants.add(variant);
		}
	}
}
