package org.rcsb.geneprot.genevariation.filters;

import org.rcsb.geneprot.genevariation.datastructures.VariantInterface;

/**
 * This Interface provides custom filtering logic based on the needs.
 * User can create the custom filter class by implementing this filter and invoke the filter based methods 
 * to apply the filter.
 * 
 * @author Yana Valasatava
 */

public interface IVariantDataFilter {

	boolean filter(VariantInterface variant);

}
