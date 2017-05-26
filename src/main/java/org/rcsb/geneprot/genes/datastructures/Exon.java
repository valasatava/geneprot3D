package org.rcsb.geneprot.genes.datastructures;

import org.biojava.nbio.core.sequence.transcription.Frame;
import org.rcsb.geneprot.genes.constants.ExonFrameOffset;

public class Exon {
	
	private int start;
	private int end;
	private ExonFrameOffset phase;

	private boolean forward;
	private Frame frame;

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

	public Frame getFrame() {
		return frame;
	}

	public void setFrame(int frame, boolean forward) {
		switch (frame) {
			case 0:
				this.frame = Frame.ONE;
				if (!forward)
					this.frame = Frame.REVERSED_ONE;
				break;
			case 1:
				this.frame = Frame.TWO;
				if (!forward)
					this.frame = Frame.REVERSED_TWO;
				break;
			case 2:
				this.frame = Frame.THREE;
				if (!forward)
					this.frame = Frame.REVERSED_THREE;
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
