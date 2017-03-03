package org.rcsb.genevariation.datastructures;

public class Mutation {
	
	private String chromosomeName;
	private String geneBankId;
	private long position;
	private String referenceAminoAcid;
	private String mutatedAminoAcid;
	
	public void setChromosomeName(String chromosomeName) {
		this.chromosomeName = chromosomeName;
	}
	public String getChromosomeName() {
		return chromosomeName;
	}
	public String getGeneBankId() {
		return geneBankId;
	}
	public void setGeneBankId(String geneBankId) {
		this.geneBankId = geneBankId;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	public long getPosition() {
		return position;
	}
	public void setRefAminoAcid(String aa) {
		this.referenceAminoAcid = aa;
	}
	public String getRefAminoAcid() {
		return referenceAminoAcid;
	}
	public void setMutAminoAcid(String aa) {
		this.mutatedAminoAcid = aa;
	}
	public String getMutAminoAcid() {
		return mutatedAminoAcid;
	}
}
