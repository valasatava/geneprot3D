package org.rcsb.genevariation.datastructures;

import java.util.ArrayList;
import java.util.List;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.constants.StrandOrientation;

public class Gene {
	
	private String chromosome;
	private int codingStart;
	private int codingEnd;
	private List<Exon> exons;
	
	private StrandOrientation orientation;
	private DNASequence sequence;
	private String name;
		
	public String getChromosome() {
		return chromosome;
	}
	
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getCodingStart() {
		return codingStart;
	}
	
	public void setCodingStart(int start) {
		this.codingStart = start;
	}
	
	public int getCodingEnd() {
		return codingEnd;
	}
	
	public void setCodingEnd(int end) {
		this.codingEnd = end;
	}
	
	public List<Exon> getExons() {
		return exons;
	}

	public void setExons(List<Exon> exons) {
		this.exons = exons;
	}

	public List<Integer> getExonStarts() {
		List<Integer> exonStarts = new ArrayList<Integer>();
		for (Exon exon : exons) {
			exonStarts.add(exon.getStart());
		}
		return exonStarts;
	}
	
	public List<Integer> getExonEnds() {
		List<Integer> exonEnds = new ArrayList<Integer>();
		for (Exon exon : exons) {
			exonEnds.add(exon.getEnd());
		}
		return exonEnds;
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
