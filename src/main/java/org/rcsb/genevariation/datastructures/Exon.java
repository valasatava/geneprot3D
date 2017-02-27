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
	
	public void setPhase(int phase) {
		switch (phase) {
			case -1:
				this.phase = ExonFrameOffset.PHASE_UTR;
				break;
			case 0:
				this.phase = ExonFrameOffset.PHASE_ZERO;
				break;
			case 1:
				this.phase = ExonFrameOffset.PHASE_ONE;
				break;
			case 2:
				this.phase = ExonFrameOffset.PHASE_TWO;
				break;
		}
	}
	
	public void setPhase(ExonFrameOffset phase) {
		this.phase = phase;
	}
	
	public int getLength() {
		return end-start;
	}
}
