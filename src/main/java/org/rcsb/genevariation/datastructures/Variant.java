package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public interface Variant {
	
	String getChromosome();
	long getPosition();
	VariantType getType();
	
	void setVariation(String ref, String alt);

}
