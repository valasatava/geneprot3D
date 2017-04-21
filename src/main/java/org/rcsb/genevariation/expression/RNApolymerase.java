package org.rcsb.genevariation.expression;

import com.google.common.collect.Range;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.RNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.biojava.nbio.genome.parsers.twobit.TwoBitParser;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Transcript;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RNApolymerase implements Serializable  {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6996001236685762558L;

	public TwoBitParser parser;
	private final static String userHome = System.getProperty("user.home");
	private final static String DEFAULT_GENOME_URI = userHome+"/data/genevariation/hg38.2bit";	
	public static final String DEFAULT_MAPPING_URL="http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refGene.txt.gz";
	
	public RNApolymerase() throws Exception {
		//readGenome();
	}
	
	public RNApolymerase(String chr) throws Exception {
		readGenome();
		setChromosome(chr);
	}
	
	/**
	 * Reads a genome from a locally stored .2bit file (hard-coded URI). 
	 */
	private void readGenome() throws Exception {
		File f = new File(DEFAULT_GENOME_URI);
		this.parser = new TwoBitParser(f);
	}
	
	/**
	 * Sets a chromosome number for TwoBitParser.
	 */
	public void setChromosome(String chr) throws Exception {
		parser.close();
		String[] names = parser.getSequenceNames();
		for(int i=0;i<names.length;i++) {
			if ( names[i].equals(chr) ) {
				parser.setCurrentSequence(names[i]);
				break;
			}
		}
	}
	
	public int getmRNAPositionForGeneticCoordinate(int coordinate, Transcript transcript) {
		
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
	
	public RNASequence getmRNAsequence(Transcript transcript) throws CompoundNotFoundException, IOException {
		String cs = this.getCodingSequence(transcript);
		DNASequence dna = new DNASequence(cs);
		return dna.getRNASequence();
	}
	
	public String getCodon(int cds, String codingSequence) throws IOException {
		
		int offset = cds%3;
		
		CharSequence codon = "";
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
	
	public String getCodingSequence(Transcript transcript) throws IOException, CompoundNotFoundException {
		
		List<Integer> exonStarts = transcript.getExonStarts();
		List<Integer> exonEnds = transcript.getExonEnds();
		int codingStart = transcript.getCodingStart();
		int codingEnd = transcript.getCodingEnd();
		boolean forward = true;
		if ( transcript.getOrientation().equals(StrandOrientation.REVERSE) ) {
			forward = false;
		}
		return getCodingSequence(exonStarts, exonEnds, codingStart, codingEnd, forward);
	}
	
	public String getCodingSequence(List<Integer> exonStarts, List<Integer> exonEnds, int codingStart, int codingEnd, boolean forward) throws IOException, CompoundNotFoundException {

		List<Range<Integer>> cdsRegion = ChromosomeMappingTools.getCDSRegions(exonStarts, exonEnds,
					codingStart, codingEnd);

		String transcription = "";
		for (Range<Integer> range : cdsRegion) {
			int length = range.upperEndpoint() - range.lowerEndpoint();
			transcription += parser.loadFragment(range.lowerEndpoint(), length);
		}
		if ( !forward ) {
			transcription = new StringBuilder(transcription).reverse().toString();
			DNASequence dna = new DNASequence(transcription);
			SequenceView<NucleotideCompound> compliment = dna.getComplement();
			transcription = compliment.getSequenceAsString();
		}
		return transcription;
	}
}
