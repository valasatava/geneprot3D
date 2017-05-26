package org.rcsb.geneprot.genes.expression;

import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.geneprot.genes.constants.StrandOrientation;
import org.rcsb.geneprot.genes.datastructures.Exon;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.genome.parsers.twobit.SimpleTwoBitFileProvider;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.geneprot.genes.datastructures.Transcript;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RNApolymerase implements Serializable  {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6996001236685762558L;

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
	
	public String getCodingSequence(Transcript transcript) throws Exception {

		char orientation = '+';
		if ( transcript.getOrientation().equals(StrandOrientation.REVERSE) ) {
			orientation = '-';
		}

		return getCodingSequence(transcript.getChromosomeName(),
				transcript.getExonStarts(), transcript.getExonEnds(),
				transcript.getCodingStart(), transcript.getCodingEnd(), orientation).toString();
	}
	
	public DNASequence getCodingSequence(String chromosome, List<Integer> exonStarts, List<Integer> exonEnds,
									int codingStart, int codingEnd, char orientation) throws Exception {

		File twoBitFileLocalLocation = new File(DataLocationProvider.getHumanGenomeLocation());
		SimpleTwoBitFileProvider.downloadIfNoTwoBitFileExists(twoBitFileLocalLocation, "hg38");
		TwoBitFacade twoBitFacade = new TwoBitFacade(twoBitFileLocalLocation);

		DNASequence transcribedDNASequence = ChromosomeMappingTools.getTranscriptDNASequence(twoBitFacade,
				chromosome, exonStarts, exonEnds, codingStart, codingEnd, orientation);

		return transcribedDNASequence;
	}

	public static DNASequence getCodingSequence(TwoBitFacade twoBitFacade, String chromosome, StrandOrientation orientation, List<Exon> exons) throws Exception {

		String dnaSequence = "";
		for (Exon e : exons) {
			dnaSequence += twoBitFacade.getSequence(chromosome, e.getStart()-1, e.getEnd());
		}

		if(orientation.equals(StrandOrientation.REVERSE)) {
			dnaSequence = (new StringBuilder(dnaSequence)).reverse().toString();
			DNASequence dna = new DNASequence(dnaSequence);
			SequenceView<NucleotideCompound> compliment = dna.getComplement();
			dnaSequence = compliment.getSequenceAsString();
		}
		return new DNASequence(dnaSequence.toUpperCase());
	}

	public static String getExonSequence(TwoBitFacade twoBitFacade, String chromosome, String orientation, Exon exon) throws Exception {

		String transcription = "";
		if (!orientation.equals("+")) {

			int length = ((exon.getEnd() - exon.getPhase().getValue()) - exon.getStart()) + 1;
			int correction = length % 3;
			length = length - correction;

			int start = (exon.getStart() + correction) - 1;
			int end = start + length;
			transcription = twoBitFacade.getSequence(chromosome, start, end);
			transcription = new StringBuilder(transcription).reverse().toString();
			DNASequence dna = new DNASequence(transcription);
			SequenceView<NucleotideCompound> compliment = dna.getComplement();
			transcription = compliment.getSequenceAsString();
		} else {

			int length = (exon.getEnd() - (exon.getStart() + exon.getPhase().getValue())) + 1;
			int correction = length % 3;
			length = length - correction;

			int start = (exon.getStart() + correction) - 1;
			int end = start + length;
			transcription = twoBitFacade.getSequence(chromosome, start, end);
		}
		return transcription;
	}
}
