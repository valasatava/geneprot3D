package org.rcsb.genevariation.utils;

import static org.junit.Assert.*;

import org.rcsb.geneprot.genevariation.utils.VariationUtils;
import org.junit.Test;
import org.rcsb.geneprot.genevariation.constants.VariantType;

/**
 * Class to test the utility methods
 * 
 * @author Yana Valasatava
 */
public class TestUtils {
	
	/**
	 * TestJoin a correct identification of a SNP type of variation
	 */
	@Test
	public void testCheckVariationTypeSNP() {
		VariantType type = VariationUtils.checkType("A", "T");
		assertSame(type.compareTo(VariantType.SNP), 0);
	}
	
	/**
	 * TestJoin a correct identification of a monomorphic type of variation
	 */
	@Test
	public void testCheckVariationTypeMonomorphic() {
		VariantType type = VariationUtils.checkType("A", ".");
		assertSame(type.compareTo(VariantType.MONOMORPHIC), 0);
	}
	
	/**
	 * TestJoin a correct identification of a insertion type of variation
	 */
	@Test
	public void testCheckVariationTypeInsertion() {
		VariantType type = VariationUtils.checkType("GTC","GTCT");
		assertSame(type.compareTo(VariantType.INSERTION), 0);
	}
	
	/**
	 * TestJoin a correct identification of a insertion type of variation
	 */
	@Test
	public void testCheckVariationTypeDeletion() {
		VariantType type = VariationUtils.checkType("GTC","G");
		assertSame(type.compareTo(VariantType.DELETION), 0);
	}
}
