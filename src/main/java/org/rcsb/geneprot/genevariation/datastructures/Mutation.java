package org.rcsb.geneprot.genevariation.datastructures;

public class Mutation {
	
	private String chromosomeName;
	private String geneBankId;
	private long position;

	private String uniProtId;
	private int uniProtPos;
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
	public String getUniProtId() {
		return uniProtId;
	}
	public void setUniProtId(String uniProtId) {
		this.uniProtId = uniProtId;
	}
	public int getUniProtPos() {
		return uniProtPos;
	}
	public void setUniProtPos(int uniProtPos) {
		this.uniProtPos = uniProtPos;
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
