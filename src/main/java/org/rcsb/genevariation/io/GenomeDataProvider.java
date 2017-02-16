package org.rcsb.genevariation.io;

import java.io.File;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;

/**
 * This class provides methods to retrieve genetic data from files.
 * 
 * @author Yana Valasatava
 */
public class GenomeDataProvider {
	
	static TwoBitParser parser;

	public static void readTwoBitFile(String path) throws Exception {
		File f = new File(path);
		parser = new TwoBitParser(f);
	}
	
	public static void setChromosome(int chr) throws Exception {
		
		//TODO check on haplogroups 
		
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( names[i].equals("chr"+chr) ) {
				parser.setCurrentSequence(names[i]);
				break;
			}
		}
	}
	
	public String readBaseFromChromosome(long position) throws Exception {
		return parser.loadFragment(position-1, 1);
	}
	
//	public static DNASequence readDNASequenceFromChromosome(long startPos, long endPos) {
//		
//		DNASequence dna = new DNASequence(sequence);
//		
//	}

}
