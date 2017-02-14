package org.rcsb.genevariation.sandbox;

import java.io.File;
import java.io.IOException;

import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;

public class TwoBitGeneDataConsumer {
	
	static TwoBitParser parser;
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome+"/data/genevariation/hg38.2bit";
	
	public TwoBitGeneDataConsumer() throws Exception {
		
		File f = new File(path);
		parser = new TwoBitParser(f);
	}
	
	public String readBaseFromChromosome(String chr, long position) throws Exception {
		
		String base = "";
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( !names[i].equals("chr"+chr) ) {
				continue;
			}
			parser.setCurrentSequence(names[i]);
			base = parser.loadFragment(position-1, 1);
			parser.close();
		}
		return base;
	}

	private String readForward(long position, int len) throws IOException {
		// read the forward strand of a DNA chromosome in the 5' to 3' direction (reading left-to-right)
		return parser.loadFragment(position, 3);
	}
	
	private String readReverse(long position, int len) throws IOException {
		// read the reverse strand of a DNA chromosome in the 3' to 5' direction (reading right-to-left)
		return parser.loadFragment(position-2, 3);
	}
	
	public String readCodonFromChromosome(String chr, long position, int phase, String orientation) throws Exception {
		
		String codone = "";
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( !names[i].equals("chr"+chr) ) {
				continue;
			}
			long p = 0;
			parser.setCurrentSequence(names[i]);
			if (orientation.equals("+")) {
				p = position+phase-1;
				codone = readForward(p,3);
			}
			else {
				p = position-phase+1;
				readReverse(p,3);
			}
			parser.close();
		}
		return codone;
	}
}
