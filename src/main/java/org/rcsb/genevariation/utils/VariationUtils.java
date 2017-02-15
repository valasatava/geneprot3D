package org.rcsb.genevariation.utils;

import org.rcsb.genevariation.constants.VariantType;

public class VariationUtils {
	
	public static VariantType checkType(String ref, String alt) {
		
		if ( ref.length() == alt.length() ) {
			if ( alt.equals(".") ) {
				return VariantType.MONOMORPHIC;
			}
			else {
				return VariantType.SNP;
			}
		}
		else {
			if (ref.length() < alt.length() ) {
				return VariantType.INSERTION;
			}
			else {
				return VariantType.DELETION;
			}
		}
	}
}
