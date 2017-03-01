package org.rcsb.genevariation.filters;

import org.rcsb.genevariation.datastructures.Variant;

/**
 * This Interface provides the facility to provide the custom filtering logic based on the needs. 
 * User can create the custom filter class by implementing this filter and invoke the filter based methods 
 * to apply the filter.
 * 
 * @author Yana Valasatava
 */

public interface IDataProviderFilter {

	boolean filter(Variant variant);

}
