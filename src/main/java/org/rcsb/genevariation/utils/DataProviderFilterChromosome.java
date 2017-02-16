package org.rcsb.genevariation.utils;

import org.rcsb.genevariation.datastructures.Variant;

public class DataProviderFilterChromosome implements IDataProviderFilter {

	private String chr;
	public DataProviderFilterChromosome(String chr) {
		this.chr = chr;
	}

	@Override
	public boolean filter(Variant variant) {
		if ( variant.getChromosome().equals(chr) ) { 
			return true;
		}
		return false;
	}
}
