package org.rcsb.genevariation.sandbox;

public class SnpBean {
	
	private int phase = -2;
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
