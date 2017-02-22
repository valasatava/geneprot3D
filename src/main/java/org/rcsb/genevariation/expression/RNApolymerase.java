package org.rcsb.genevariation.expression;

import java.util.List;
import java.util.stream.Collectors;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.RNASequence;
import org.rcsb.genevariation.datastructures.Transcript;

public class RNApolymerase {
	
	public static RNASequence getmRNASequence(Transcript transcript) throws CompoundNotFoundException {
		
		List<Integer> starts = transcript.getExonStarts();
		List<Integer> ends = transcript.getExonEnds();
		
		int ref = starts.get(0);
		
		List<Integer> startsCentered = starts.stream().map(t -> (t - ref)).collect(Collectors.toList());
		List<Integer> endsCentered = ends.stream().map(t -> (t - ref)).collect(Collectors.toList());
		
		int codingS = transcript.getCodingStart() - ref;
		int codingE = transcript.getCodingEnd() - ref;
		
		String transcription = transcript.getDNASequenceAsString();		
		String translation = transcription.substring(codingS, endsCentered.get(0));
		for ( int i=1; i< transcript.getExonNumber()-1; i++ ) {
			translation += transcription.substring(startsCentered.get(i), endsCentered.get(i));
		}
		translation += transcription.substring(startsCentered.get(transcript.getExonNumber()-1), codingE);
		
		DNASequence dna = new DNASequence(translation);
		return dna.getRNASequence();
	}
}
