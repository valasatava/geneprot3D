package org.rcsb.genevariation.mappers;

import org.apache.spark.api.java.function.FilterFunction;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.VcfContainer;
import org.rcsb.genevariation.utils.VariationUtils;

//FilterFunction that filters out all SNPs
public class FilterSNPs implements FilterFunction<VcfContainer> {
	private static final long serialVersionUID = 4443092527065578555L;
	@Override
	public boolean call(VcfContainer v) throws Exception {
		if ( VariationUtils.checkType(v.getOriginal(), v.getVariant()).compareTo(VariantType.SNP) == 0 ) {
			
			return true;
		}
		return false;
	}
}
