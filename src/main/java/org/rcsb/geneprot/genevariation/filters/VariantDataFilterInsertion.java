package org.rcsb.geneprot.genevariation.filters;

import org.rcsb.geneprot.genevariation.constants.VariantType;
import org.rcsb.geneprot.genevariation.datastructures.VariantInterface;

public class VariantDataFilterInsertion implements IVariantDataFilter {

	@Override
	public boolean filter(VariantInterface variant) {
		if ( variant.getType().compareTo(VariantType.INSERTION) == 0 ) { 
			return true;
		}
		return false;
	}
}
