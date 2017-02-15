package org.rcsb.genevariation.io;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.nio.file.Path;

import org.pharmgkb.parser.vcf.VcfParser;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.SNP;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.utils.VariationUtils;

public class VariantsDataProvider {
	
	private List<Variant> variants;
	
	public VariantsDataProvider() {
		variants = new ArrayList<Variant>();
	}
	
	public void readVariantsFromVCF(Path filepath) throws IOException {

		VcfParser parser = new VcfParser.Builder().fromFile(filepath).parseWith((metadata, position, sampleData) -> {
			
			String chromosome = position.getChromosome();
			long pos = position.getPosition();
			String ref = position.getRef();
			List<String> alts = position.getAltBases();
			
			for (String alt : alts) {

				Variant variant = null;
				VariantType type = VariationUtils.checkType(ref, alt);
				
				switch (type) {
				case SNP:
					variant = new SNP(chromosome, pos, type);
					variant.setVariation(ref, alt);
					break;
				
				case MONOMORPHIC:
					System.out.println("MONOMORPHIC");
					break;
					
				case INSERTION:
					System.out.println("INSERTION");
					break;

				case DELETION:
					System.out.println("DELETION");
					break;
					
				default:
					System.out.println("DEFAULT");
					break;
				}
				addVariant(variant);
			}
		}).build();
		parser.parse();
	}

	public List<Variant> getSNPs() {
		List<Variant> snps = new ArrayList<Variant>();
		for (Variant variant : variants) {
			if ( variant.getType().compareTo(VariantType.SNP) == 0 ) {
				snps.add(variant);
			}
		}
		return snps;
	}
	
	private void addVariant(Variant variant) {
		this.variants.add(variant);
	}
	
	public List<Variant> getVariants() {
		return variants;
	}
}
