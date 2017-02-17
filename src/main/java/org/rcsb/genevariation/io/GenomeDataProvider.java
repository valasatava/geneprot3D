package org.rcsb.genevariation.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.biojava.nbio.core.util.InputStreamProvider;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Exon;

/**
 * This class provides methods to retrieve genetic data from files.
 * 
 * @author Yana Valasatava
 */
public class GenomeDataProvider {
	
	static TwoBitParser parser;
	
	private final static String userHome = System.getProperty("user.home");
	private final static String DEFAULT_GENOME_URI = userHome+"/data/genevariation/hg38.2bit";
	
	public static final String DEFAULT_MAPPING_URL="http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";
	
	public static void readTwoBitFile(String path) throws Exception {
		File f = new File(path);
		parser = new TwoBitParser(f);
	}
	
	/**
	 * Reads a genome from a locally stored .2bit file (hard-coded URI).
	 * 
	 */
	public static void readGenome() throws Exception {
		File f = new File(DEFAULT_GENOME_URI);
		parser = new TwoBitParser(f);
	}
	
	/**
	 * Sets a chromosome number for TwoBitParser.
	 */
	public static void setChromosome(String chr) throws Exception {
		
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( names[i].equals("chr"+chr) ) {
				parser.setCurrentSequence(names[i]);
				break;
			}
		}
	}
	
	/**
	 * Gets a List of Exon instances.
	 * @throws Exception 
	 * 
	 */
	public static List<Exon> getExonsFromChromosome(String chr) throws Exception {
		
		readGenome();
		setChromosome(chr);
		
		URL url = new URL(DEFAULT_MAPPING_URL);

		InputStreamProvider prov = new InputStreamProvider();
		InputStream inStream = prov.getInputStream(url);
		
		List<Exon> exons = new ArrayList<Exon>();
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings(inStream);
		
		for (GeneChromosomePosition gcp : gcps) {
			
			List<Integer> starts = gcp.getExonStarts();
			List<Integer> ends = gcp.getExonEnds();

			for (int i=0; i<starts.size(); i++) {
				
				Exon e = new Exon(starts.get(i), ends.get(i));
				
				e.setChromosome(gcp.getChromosome());

				switch (gcp.getOrientation()) {
				case '+':
					e.setOrientation(StrandOrientation.FORWARD);
					break;
				case '-':
					e.setOrientation(StrandOrientation.REVERSE);
					break;
				}
				// set DNA sequence
				int len = ends.get(i) - starts.get(i);
				String sequence = parser.loadFragment(starts.get(i) - 1, len);
				e.setDNASequence(sequence);
				
				exons.add(e);
			}
		}
		return exons;
	}
	
	public static List<Exon> filterExonsHavingPosition(List<Exon> exons, long position) {
		List<Exon> filtered = exons.stream().filter(t -> (t.getStart() <= position 
				&& t.getEnd() >= position)).collect(Collectors.toList());
		return filtered; 
	}
	
	public static List<Exon> getExonBoundariesForPosition(String chr, long position) throws Exception {
		List<Exon> exons = getExonsFromChromosome(chr);
		return filterExonsHavingPosition(exons, position);
	}
	
	public String readBaseFromChromosome(long position) throws Exception {
		return parser.loadFragment(position-1, 1);
	}
	
	public static String readDNASequenceFromChromosome(long startPos, long endPos) {
		return null;		
	}

}
