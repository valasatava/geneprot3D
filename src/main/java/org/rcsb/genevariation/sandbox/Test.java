package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.pharmgkb.parser.vcf.VcfParser;

public class Test {
	
	private final static String userHome = System.getProperty("user.home");
	
	public static void main(String[] args) throws IOException {
		
		String path = userHome+"/data/genevariation/vcfExample.vcf";
		Path file = Paths.get(path);
				
		VcfParser parser = new VcfParser
				.Builder()
				.fromFile(file)
		        .parseWith((metadata, position, sampleData) -> {
		        	  System.out.println(position.getChromosome() +" " + position.getPosition()+" "+position.getAltBases().toString());
		         })
				.build();
		parser.parse();
		
	}
}
