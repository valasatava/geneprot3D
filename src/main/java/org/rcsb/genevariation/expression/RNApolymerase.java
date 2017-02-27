package org.rcsb.genevariation.expression;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.RNASequence;
import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Transcript;

import com.google.common.collect.Range;

public class RNApolymerase {
	
	static TwoBitParser parser;
	private final static String userHome = System.getProperty("user.home");
	private final static String DEFAULT_GENOME_URI = userHome+"/data/genevariation/hg38.2bit";
	
	public static final String DEFAULT_MAPPING_URL="http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refGene.txt.gz";
	
	public RNApolymerase() throws Exception {
		readGenome();
	}
	
	public RNApolymerase(String chr) throws Exception {
		readGenome();
		setChromosome(chr);
	}
	
	/**
	 * Reads a genome from a locally stored .2bit file (hard-coded URI).
	 * 
	 */
	private static void readGenome() throws Exception {
		File f = new File(DEFAULT_GENOME_URI);
		parser = new TwoBitParser(f);
	}
	
	/**
	 * Sets a chromosome number for TwoBitParser.
	 */
	public static void setChromosome(String chr) throws Exception {
		
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( names[i].equals(chr) ) {
				parser.setCurrentSequence(names[i]);
				break;
			}
		}
	}
	
	public static int getmRNAPositionForGeneticCoordinate(int coordinate, Transcript transcript) {
		
		if ( transcript.getOrientation().equals(StrandOrientation.FORWARD)) {
        	return ChromosomeMappingTools.getCDSPosForward(coordinate,
        			transcript.getExonStarts(),
        			transcript.getExonEnds(),
            		transcript.getCodingStart(),
            		transcript.getCodingEnd());
		}
        return ChromosomeMappingTools.getCDSPosReverse(coordinate,
        		transcript.getExonStarts(),
        		transcript.getExonEnds(),
        		transcript.getCodingStart(),
        		transcript.getCodingEnd());
	}
	
	public static RNASequence getmRNAsequence(Transcript transcript) throws CompoundNotFoundException {
		
		
		DNASequence dna = new DNASequence("");
		return dna.getRNASequence();
		
	}
	
	public String getCodingCodon(int coordinate, Transcript transcript) throws IOException {
		
		int cds = getmRNAPositionForGeneticCoordinate(coordinate, transcript);
		String codingSequence = getCodingSequence(transcript);
		
		CharSequence codon = "";
		int offset = cds%3;
		if (offset==0) {
			codon = codingSequence.subSequence(cds-3, cds);
		}
		if (offset==1) {
			codon = codingSequence.subSequence(cds-1, cds+2);
		}
		else if (offset==2) {
			codon = codingSequence.subSequence(cds-2, cds+1);
		}
		return codon.toString();
	}
	
	public String getCodingSequence(Transcript transcript) throws IOException {
		
		List<Range<Integer>> cdsRegion = ChromosomeMappingTools.getCDSRegionsReverse(transcript.getExonStarts(), transcript.getExonEnds(), 
				transcript.getCodingStart(), transcript.getCodingEnd());
		
		String transcription = "";
		for (Range<Integer> range : cdsRegion) {
			int length = range.upperEndpoint() - range.lowerEndpoint();
			transcription += parser.loadFragment(range.lowerEndpoint(), length);
		}
		return transcription;
	}
}
