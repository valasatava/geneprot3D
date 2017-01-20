package org.rcsb.genevariation.sandbox;

import java.io.File;

import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;

public class TwoBitParserTest {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome+"/data/genevariation/hg38.2bit";
	
	public static void main(String[] args) throws Exception {
		seq();
	}
	
	public static void seq() throws Exception {
		
		File f = new File(path);
		TwoBitParser parser = new TwoBitParser(f);
		
		String chr = "21";
		
		String base = "";
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( !names[i].equals("chr"+chr) ) {
				continue;
			}
			parser.setCurrentSequence(names[i]);
			base = parser.loadFragment(10413613-1, 20);
			System.out.println(base);
			
			String base2 = parser.loadFragment(10413635-1, 20);
			System.out.println(base2);
			parser.close();
		}
		
	}
}
