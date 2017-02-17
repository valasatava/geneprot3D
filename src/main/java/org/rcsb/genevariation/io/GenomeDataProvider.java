package org.rcsb.genevariation.io;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.biojava.nbio.core.util.InputStreamProvider;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Exon;
import org.rcsb.genevariation.datastructures.Gene;

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
	public static List<Gene> getGenesFromChromosome(String chr) throws Exception {
		
		readGenome();
		setChromosome(chr);
		
		URL url = new URL(DEFAULT_MAPPING_URL);

		InputStreamProvider prov = new InputStreamProvider();
		InputStream inStream = prov.getInputStream(url);
		
		List<Gene> genes = new ArrayList<Gene>();
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings(inStream);
		
		String sequence;
		for (GeneChromosomePosition gcp : gcps) {
			
			if ( !gcp.getChromosome().equals("chr"+chr) ) {
				continue;
			}
			
			Gene gene = new Gene();
			gene.setChromosome(gcp.getChromosome());
			
			gene.setName(gcp.getGeneName());
			gene.setCodingStart(gcp.getCdsStart());
			gene.setCodingEnd(gcp.getCdsEnd());
			
			switch (gcp.getOrientation()) {
			case '+':
				gene.setOrientation(StrandOrientation.FORWARD);
				break;
			case '-':
				gene.setOrientation(StrandOrientation.REVERSE);
				break;
			}
			
			List<Integer> starts = gcp.getExonStarts();
			List<Integer> ends = gcp.getExonEnds();
			
			List<Exon> exons = new ArrayList<Exon>();
			for ( int i=0; i < starts.size(); i++ ) {
				Exon e = new Exon();
				e.setStart(starts.get(i));
				e.setEnd(ends.get(i));
				exons.add(e);
			}
			gene.setExons(exons);
			
			// DNA sequence covered by a gene: exons and introns
			Integer start = starts.get(0);
			Integer end = ends.get(ends.size()-1);
			
			int len = end - start;
			sequence = parser.loadFragment(start-1, len);
			gene.setDNASequence(sequence);

			genes.add(gene);
		}
		return genes;
	}
	
	public static List<Gene> getGenesAtPosition(List<Gene> genes, long position) {
		
		List<Gene> filtered = new ArrayList<Gene>();
		for (Gene gene : genes) {
			List<Exon> exons = gene.getExons();
			for (Exon exon : exons) {
				if ( (exon.getStart() <= position) && (position <= exon.getEnd()) ) {
					filtered.add(gene);
					break;
				}
			}
		}
		return filtered; 
	}
	
	public static List<Gene> getExonBoundariesForPosition(String chr, long position) throws Exception {
		List<Gene> exons = getGenesFromChromosome(chr);
		return getGenesAtPosition(exons, position);
	}
	
	public String readBaseFromChromosome(long position) throws Exception {
		return parser.loadFragment(position-1, 1);
	}
	
	public static String readDNASequenceFromChromosome(long startPos, long endPos) {
		return null;		
	}
}
