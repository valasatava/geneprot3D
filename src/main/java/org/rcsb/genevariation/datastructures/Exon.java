package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.ExonFrameOffset;

public class Exon {
	
	private int start;
	private int end;
	private ExonFrameOffset phase;
	
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
	public ExonFrameOffset getPhase() {
		return phase;
	}
	public void setPhase(ExonFrameOffset phase) {
		this.phase = phase;
	}
	
	public int getLength() {
		return end-start;
	}
}
