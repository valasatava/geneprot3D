package org.rcsb.genevariation.filters;

import org.rcsb.genevariation.datastructures.VariantInterface;

public class VariantDataFilterChromosome implements IVariantDataFilter {

	private String chr;
	public VariantDataFilterChromosome(String chr) {
		this.chr = chr;
	}

	@Override
	public boolean filter(VariantInterface variant) {
		if ( variant.getChromosomeName().equals(chr) ) { 
			return true;
		}
		return false;
	}
}
