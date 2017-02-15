package org.rcsb.genevariation.utils;

import org.rcsb.genevariation.datastructures.Variant;

public class DataProviderFilterChromosome implements IDataProviderFilter {
	
	private int chr;
	public DataProviderFilterChromosome(int chr) {
		this.chr = chr;
	}
	
	@Override
	public boolean filter(Variant variant) {
		if ( variant.getChromosome().equals(Integer.toString(chr)) ) { 
			return true;
		}
		return false;
	}
}
