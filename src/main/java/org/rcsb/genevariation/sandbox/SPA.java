package org.rcsb.genevariation.sandbox;

public class SPA {
	
	private long position;
	private SPAType type;
	
	public SPA() {
		type = SPAType.DAN;
		position = 1;
	}
	
	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public SPAType getType() {
		return type;
	}

	public void setType(SPAType type) {
		this.type = type;
	}
}
