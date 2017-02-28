package org.rcsb.genevariation.datastructures;

public class Mutation {
	
	private String chromosomeName;
	private long position;
	private String refAA;
	private String mutAA;
	
	public String getChromosomeName() {
		return chromosomeName;
	}
	public long getPosition() {
		return position;
	}
	public String getRefAA() {
		return refAA;
	}
	public String getMutAA() {
		return mutAA;
	}
	public void setChromosomeName(String chromosomeName) {
		this.chromosomeName = chromosomeName;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	public void setRefAA(String refAA) {
		this.refAA = refAA;
	}
	public void setMutAA(String mutAA) {
		this.mutAA = mutAA;
	}
}
