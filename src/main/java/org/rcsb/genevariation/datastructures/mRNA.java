package org.rcsb.genevariation.datastructures;

import java.util.List;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.constants.StrandOrientation;

public class mRNA {
	
	private String chromosome;
	private int codingStart;
	private int codingEnd;
	private List<Integer> exonStarts;
	private int exonEnds;
	private StrandOrientation orientation;
	private DNASequence sequence;
	
	public mRNA() { }
	
	public mRNA(int start, int end) {
		setStart(start);
		setEnd(end);
	}
	
	public String getChromosome() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}
	public int getStart() {
		return exonStarts;
	}
	public void setStart(int start) {
		this.exonStarts = start;
	}
	public int getEnd() {
		return exonEnds;
	}
	public void setEnd(int end) {
		this.exonEnds = end;
	}
	public StrandOrientation getOrientation() {
		return orientation;
	}
	public void setOrientation(StrandOrientation orientation) {
		this.orientation = orientation;
	}
	public DNASequence getDNASequence() {
		return sequence;
	}
	public String getDNASequenceAsString() {
		return sequence.getSequenceAsString();
	}
	public void setDNASequence(String sequence) throws CompoundNotFoundException {
		
		DNASequence dna = new DNASequence(sequence);
		
		if (orientation.equals(StrandOrientation.FORWARD)) {
			this.sequence = dna;
		}
		else if (orientation.equals(StrandOrientation.REVERSE)) {
			SequenceView<NucleotideCompound> reverse = dna.getReverseComplement();
			String reverseSequence = reverse.getSequenceAsString();
			this.sequence = new DNASequence(reverseSequence);
		}
	}
	public void setDNASequence(DNASequence sequence) {
		this.sequence = sequence;
	}
}
