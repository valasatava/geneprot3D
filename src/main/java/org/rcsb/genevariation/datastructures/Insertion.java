package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public class Insertion extends VariantImpl {
	
	private String refBase;
	private String[] insBases;
	
	public Insertion(String chromosome, long position, VariantType type) {
		super(chromosome, position, type);
	}

	@Override
	public void setVariation(String ref, String alt) {
		setRefBase(ref);
		setInsBase(alt);
	}

	private void setRefBase(String refBase) {
		this.refBase = refBase;
	}
	private void setInsBase(String alt) {
		this.insBases = new String[alt.length()-1];
		String[] bases = alt.split("");
		for ( int i=1;i<alt.length();i++ ) {
			this.insBases[i-1]=bases[i];
		}
	}
	
	public String getRefBase() {
		return refBase;
	}
	public String[] getAltBaseArray() {
		return insBases;
	}
	public String getAltBase() {
		return insBases.toString();
	}
}
