package org.rcsb.geneprot.genevariation.filters;

import org.rcsb.geneprot.genevariation.constants.VariantType;
import org.rcsb.geneprot.genevariation.datastructures.VariantInterface;

public class VariantDataFilterSNP implements IVariantDataFilter {

	@Override
	public boolean filter(VariantInterface variant) {
		if ( variant.getType().compareTo(VariantType.SNP) == 0 ) {
			return true;
		}
		return false;
	}
}
