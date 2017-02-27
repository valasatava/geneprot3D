package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public interface Variant {
	
	String getChromosomeName();
	long getPosition();
	VariantType getType();
	
	void setVariation(String ref, String alt);
	
	String getRefBase();
	String getAltBase();
}
