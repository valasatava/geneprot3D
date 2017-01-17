package org.rcsb.genevariation.sandbox;

import java.io.File;

import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;

public class TwoBitGeneDataConsumer {
	
	static TwoBitParser parser;
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome+"/data/genevariation/hg38.2bit";
	
	public static void main(String[] args) throws Exception {
		
	}
	
	public TwoBitGeneDataConsumer() throws Exception {
		
		File f = new File(path);
		parser = new TwoBitParser(f);
	}
	
	public String readCodonFromChromosome(String chr, long position, int phase) throws Exception {
		
		String codone = "";
		
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( !names[i].equals("chr"+chr) ) {
				continue;
			}
			long p = position+phase;
			parser.setCurrentSequence(names[i]);
			codone = parser.loadFragment(p, 3);
			parser.close();
		}
		return codone;
	}
}
