package org.rcsb.genevariation.io;

import java.io.File;
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
import org.rcsb.genevariation.datastructures.mRNA;

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
	public static List<mRNA> getExonsFromChromosome(String chr) throws Exception {
		
		readGenome();
		setChromosome(chr);
		
		URL url = new URL(DEFAULT_MAPPING_URL);

		InputStreamProvider prov = new InputStreamProvider();
		InputStream inStream = prov.getInputStream(url);
		
		List<mRNA> exons = new ArrayList<mRNA>();
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings(inStream);
		
		for (GeneChromosomePosition gcp : gcps) {
			
			Integer cdsStart = gcp.getCdsStart();
			Integer cdsEnd = gcp.getCdsEnd();
			
			List<Integer> starts = gcp.getExonStarts();
			List<Integer> ends = gcp.getExonEnds();
			
			mRNA e = new mRNA();
			e.setChromosome(gcp.getChromosome());
			
			// set the orientation of the DNA strand
			switch (gcp.getOrientation()) {
			case '+':
				e.setOrientation(StrandOrientation.FORWARD);
				break;
			case '-':
				e.setOrientation(StrandOrientation.REVERSE);
				break;
			}
			
			// get the mRNA sequence from DNA sequence for a given gene
			for (int i=0; i < starts.size(); i++) {
				
				// set the exon boundaries for a gene
				mRNA e = new mRNA(starts.get(i), ends.get(i));
				

				
				
				// set the DNA sequence
				int len = ends.get(i) - starts.get(i);
				String sequence = parser.loadFragment(starts.get(i) - 1, len);
				e.setDNASequence(sequence);
				
				
			}
			
			exons.add(e);
		}
		return exons;
	}
	
	public static List<mRNA> filterExonsHavingPosition(List<mRNA> exons, long position) {
		List<mRNA> filtered = exons.stream().filter(t -> (t.getStart() <= position 
				&& t.getEnd() >= position)).collect(Collectors.toList());
		return filtered; 
	}
	
	public static List<mRNA> getExonBoundariesForPosition(String chr, long position) throws Exception {
		List<mRNA> exons = getExonsFromChromosome(chr);
		return filterExonsHavingPosition(exons, position);
	}
	
	public String readBaseFromChromosome(long position) throws Exception {
		return parser.loadFragment(position-1, 1);
	}
	
	public static String readDNASequenceFromChromosome(long startPos, long endPos) {
		return null;		
	}

}
