package org.rcsb.geneprot.genevariation.mapfunctions;

import org.apache.spark.api.java.function.FilterFunction;
import org.rcsb.geneprot.genevariation.constants.VariantType;
import org.rcsb.geneprot.genevariation.datastructures.VcfContainer;
import org.rcsb.geneprot.genevariation.utils.VariationUtils;

//FilterFunction that filters out all SNPs
public class FilterSNPs implements FilterFunction<VcfContainer> {
	private static final long serialVersionUID = 4443092527065578555L;
	@Override
	public boolean call(VcfContainer v) throws Exception {
		if ( VariationUtils.checkType(v.getOriginal(), v.getVariant()).compareTo(VariantType.SNP) == 0 ) {
			System.out.println(v.getChromosome()+" "+v.getPosition());
			return true;
		}
		return false;
	}
}
