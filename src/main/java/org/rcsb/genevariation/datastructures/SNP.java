package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public class SNP implements Variant {
	
	private String chromosome;
	private long position;
	private VariantType type;

	private String refBase;
	private String altBase;
	
	public SNP() {}
	
	public SNP(String chromosome, long position, VariantType type) {
		setChromosome(chromosome);
		setPosition(position);
		setType(type);
	}

	public String getChromosome() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}

	public VariantType getType() {
		return type;
	}
	public void setType(VariantType type) {
		this.type = type;
	}
	
	@Override
	public void setVariation(String ref, String alt) {
		setRefBase(ref);
		setAltBase(alt);
	}

	private void setRefBase(String refBase) {
		this.refBase = refBase;
	}
	private void setAltBase(String altBase) {
		this.altBase = altBase;
	}
	
	public String getRefBase() {
		return refBase;
	}
	public String getAltBase() {
		return altBase;
	}
}
