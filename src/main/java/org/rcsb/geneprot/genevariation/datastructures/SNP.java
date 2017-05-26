package org.rcsb.geneprot.genevariation.datastructures;

import java.io.Serializable;

import org.rcsb.geneprot.genevariation.constants.VariantType;
import org.rcsb.geneprot.genevariation.utils.VariationUtils;

public class SNP extends Variant implements Serializable {
	
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
		if (isReverse()) {
			return VariationUtils.reverseComplimentaryBase(refBase);
		}
		return refBase;
	}
	public String getAltBase() {
		if (isReverse()) {
			return VariationUtils.reverseComplimentaryBase(altBase);
		}
		return altBase;
	}
}
