package org.rcsb.genevariation.expression;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.RNASequence;

public class Ribosome {
	
	public static String getProteinSequence(String sequence) throws CompoundNotFoundException {
		DNASequence dna = new DNASequence(sequence);
		RNASequence mRNA = dna.getRNASequence();
		return mRNA.getProteinSequence().toString();
	}
	
	public static ProteinSequence getProteinSequence(RNASequence mRNA) throws CompoundNotFoundException {
		return mRNA.getProteinSequence();
	}
	
	public static String getProteinSequenceAsString(RNASequence mRNA) throws CompoundNotFoundException {
		ProteinSequence protein = getProteinSequence(mRNA);
		return protein.getSequenceAsString();
	}
}
