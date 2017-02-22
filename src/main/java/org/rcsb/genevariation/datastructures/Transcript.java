package org.rcsb.genevariation.datastructures;

import java.util.ArrayList;
import java.util.List;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.genevariation.constants.StrandOrientation;

public class Transcript {
	
	private String geneBankId;
	private int codingStart;
	private int codingEnd;
	private List<Exon> exons;
	private StrandOrientation orientation;
	private DNASequence sequence;
	
	public String getGeneBankId() {
		return geneBankId;
	}

	public void setGeneBankId(String geneBankId) {
		this.geneBankId = geneBankId;
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
	
	public int getExonNumber() {
		return exons.size();
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
	
	public List<Integer> getBounderies() {
		List<Integer> bounderies = new ArrayList<Integer>();
		bounderies.add(exons.get(0).getStart()+1);
		bounderies.add(exons.get(exons.size()-1).getEnd());
		return bounderies;
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
		
		if (getOrientation().equals(StrandOrientation.FORWARD)) {
			this.sequence = dna;
		}
		else if (getOrientation().equals(StrandOrientation.REVERSE)) {
			SequenceView<NucleotideCompound> reverse = dna.getReverseComplement();
			String reverseSequence = reverse.getSequenceAsString();
			this.sequence = new DNASequence(reverseSequence);
		}
	}
	
	public void setDNASequence(DNASequence sequence) {
		this.sequence = sequence;
	}
}
