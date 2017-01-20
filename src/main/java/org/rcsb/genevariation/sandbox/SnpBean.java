package org.rcsb.genevariation.sandbox;

public class SnpBean {
	
	// overlap between an exon and previous intron 
	private int phase = -2;
	// a gene can live on a DNA strand in one of two orientations: forward ("+") and reverse ("-")
	private String orientation = "";
	private String uniProtId = "";
	
	public SnpBean(int phase, String orientation, String uniProtId) {
		setPhase(phase);
		setOrientation(orientation);
		setUniProtId(uniProtId);
	}
	
	public int getPhase() {
		return phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	public String getOrientation() {
		return orientation;
	}

	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}

	public String getUniProtId() {
		return uniProtId;
	}

	public void setUniProtId(String uniProtId) {
		this.uniProtId = uniProtId;
	}
}
