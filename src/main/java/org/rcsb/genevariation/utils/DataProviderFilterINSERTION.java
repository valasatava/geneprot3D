package org.rcsb.genevariation.utils;

import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.Variant;

public class DataProviderFilterINSERTION implements IDataProviderFilter {

	@Override
	public boolean filter(Variant variant) {
		if ( variant.getType().compareTo(VariantType.INSERTION) == 0 ) { 
			return true;
		}
		return false;
	}
}
