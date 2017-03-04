package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public class Deletion extends Variant {
	
	private String refBase;
	private String altBase;
	
	public Deletion(String chromosome, long position, VariantType type) {
		super(chromosome, position, type);
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
