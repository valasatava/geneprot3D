package org.rcsb.genevariation.datastructures;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.constants.StrandOrientation;

public class Exon {
	
	private String chromosome;
	private int start;
	private int end;
	private StrandOrientation orientation;
	private DNASequence sequence;
	
	public Exon() { }
	
	public Exon(int start, int end) {
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
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
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
