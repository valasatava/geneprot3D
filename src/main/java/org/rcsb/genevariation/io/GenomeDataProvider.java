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
import org.rcsb.genevariation.datastructures.Gene;
import org.rcsb.genevariation.datastructures.Transcript;

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
	
	public static List<GeneChromosomePosition> getGeneChromosomePositions() throws IOException {
		
		URL url = new URL(DEFAULT_MAPPING_URL);

		InputStreamProvider prov = new InputStreamProvider();
		InputStream inStream = prov.getInputStream(url);
		
		return GeneChromosomePositionParser.getChromosomeMappings(inStream);
	}
	
	/**
	 * Gets a list of genes on a given chromosome
	 * 
	 * @throws Exception
	 */
	public static List<Gene> getGenesFromChromosome(String chr) throws Exception {
		
		readGenome();
		setChromosome(chr);

		List<Gene> genes = new ArrayList<Gene>();
		
		List<GeneChromosomePosition> gcps = getGeneChromosomePositions();
		
		for (GeneChromosomePosition gcp : gcps) {
			
			if ( !gcp.getChromosome().equals("chr"+chr) ) {
				continue;
			}

			if ( !gcp.getGeneName().contains("HHIPL1") ) {
				continue;
			}
			
			Gene gene;
			List<Gene> gList = genes.stream().filter(t -> t.getName().equals(gcp.getGeneName())).collect(Collectors.toList());
			if ( gList.size() == 0 ) {
				gene = new Gene();
				gene.setChromosome(gcp.getChromosome());
				gene.setName(gcp.getGeneName());
				genes.add(gene);
			}
			else {
				gene = gList.get(0);
			}
			
			Transcript transcript = new Transcript();
			transcript.setGeneBankId(gcp.getGenebankId());
			
			transcript.setCodingStart(gcp.getCdsStart());
			transcript.setCodingEnd(gcp.getCdsEnd());
			
			List<Integer> starts = gcp.getExonStarts();
			List<Integer> ends = gcp.getExonEnds();
			
			List<Exon> exons = new ArrayList<Exon>();
			for ( int i=0; i < starts.size(); i++ ) {
				Exon e = new Exon();
				e.setStart(starts.get(i));
				e.setEnd(ends.get(i));
				exons.add(e);
			}
			transcript.setExons(exons);
			
			switch (gcp.getOrientation()) {
			case '+':
				transcript.setOrientation(StrandOrientation.FORWARD);
				break;
			case '-':
				transcript.setOrientation(StrandOrientation.REVERSE);
				break;
			}	
			// DNA sequence covered by a gene: exons and introns
			Integer start = starts.get(0);
			Integer end = ends.get(ends.size()-1);
			
			int len = end - start;
			String sequence = parser.loadFragment(start, len);
			transcript.setDNASequence(sequence);
			
			gene.addTranscript(transcript);
		}
		return genes;
	}
		
	public String readBaseFromChromosome(long position) throws Exception {
		return parser.loadFragment(position-1, 1);
	}
	
	public static String readDNASequenceFromChromosome(long startPos, long endPos) {
		return null;		
	}
}
