package org.rcsb.genevariation.datastructures;

import java.io.Serializable;

import org.rcsb.genevariation.constants.VariantType;

public class SNP extends VariantImpl implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2668841493971870661L;

	private String refBase;
	private String altBase;
	
	public SNP() {
		super();
	}
	
	public SNP(String chromosome, long position, VariantType type) {
		super(chromosome, position, type);
	}
	
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
