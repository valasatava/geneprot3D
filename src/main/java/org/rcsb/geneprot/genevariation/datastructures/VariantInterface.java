package org.rcsb.geneprot.genevariation.datastructures;

import org.rcsb.geneprot.genevariation.constants.VariantType;

public interface VariantInterface {
	
	String getChromosomeName();
	long getPosition();
	VariantType getType();
	
	void setVariation(String ref, String alt);
	
	String getRefBase();
	String getAltBase();
	
	void setReverse(boolean reverse);
	boolean isReverse();
}
