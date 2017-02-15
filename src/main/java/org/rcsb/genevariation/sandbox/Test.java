package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.io.VariantsDataProvider;

public class Test {
	
	private final static String userHome = System.getProperty("user.home");
	
	public static void main(String[] args) throws IOException {
		
		String path = userHome + "/data/genevariation/vcfExample.vcf";
		Path file = Paths.get(path);
		
		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		List<Variant> snps = vdp.getSNPs();
		for (Variant snp : snps) {
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
}
