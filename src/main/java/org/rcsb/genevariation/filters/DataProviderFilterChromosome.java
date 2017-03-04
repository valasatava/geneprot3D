package org.rcsb.genevariation.filters;

import org.rcsb.genevariation.datastructures.VariantInterface;

public class DataProviderFilterChromosome implements IDataProviderFilter {

	private String chr;
	public DataProviderFilterChromosome(String chr) {
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
