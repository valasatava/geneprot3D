package org.rcsb.geneprot.genes.datastructures;

import org.rcsb.geneprot.genes.constants.StrandOrientation;

import java.util.ArrayList;
import java.util.List;

public class Transcript {
	
	private String chromosomeName; // e.g., chr21
	private String geneName;
	private String geneBankId;
	
	private StrandOrientation orientation;
	
	private int exonsCount;
	private List<Exon> exons;
	
	private int codingStart;
	private int codingEnd;

	public Transcript() {}

	public Transcript(Gene gene) {
		setChromosomeName(gene.getChromosome());
		setOrientation(gene.getOrientation());
	}

	public String getChromosomeName() {
		return chromosomeName;
	}

	public void setChromosomeName(String chromosomeName) {
		this.chromosomeName = chromosomeName;
	}

	public String getGeneName() {
		return geneName;
	}

	public void setGeneName(String geneName) {
		this.geneName = geneName;
	}

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
	
	public int getExonsCount() {
		return exonsCount;
	}
	
	public void setExonsCount() {
		this.exonsCount = exons.size();
	}
	
	public void setExonsCount(int exonsCount) {
		this.exonsCount = exonsCount;
	}

	public int getNumberOfExons() {
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
	
	public void setOrientation(String orientation) {
		if (orientation.equals("+")) {
			this.orientation = StrandOrientation.FORWARD;
		}
		else {
			this.orientation = StrandOrientation.REVERSE;
		}
	}
	
	public void setOrientation(StrandOrientation orientation) {
		this.orientation = orientation;
	}

	public int getExonIndexByStartPos(int start) {
		if (getExonStarts().contains(start))
			return getExonStarts().indexOf(start);
		return -1;
	}

	public List<Exon> getExonsInRange(int i1, int i2) {
		return getExons().subList(i1,i2+1);
	}
}
