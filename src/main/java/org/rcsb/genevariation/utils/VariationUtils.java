package org.rcsb.genevariation.utils;

import java.io.IOException;

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
	
	public static String mutateCodonForward(int cds, String codon, String mutation) throws IOException {
		
		String codonMut="";		
		int offset = cds%3;
		if (offset==0) {
			codonMut = codon.substring(0, 2)+mutation;
		}
		else if (offset==1) {
			codonMut = mutation+codon.substring(1, 3);
		}
		else if (offset==2) {
			codonMut = codon.substring(0, 1)+mutation+codon.substring(2, 3);
		}
		return codonMut;
	}
	
	public static String mutateCodonReverse(int cds, String codon, String mutation) throws IOException {
		
		String codonMut="";		
		int offset = cds%3;
		if (offset==1) {
			codonMut = codon.substring(0, 2)+mutation;
		}
		else if (offset==0) {
			codonMut = mutation+codon.substring(1, 3);
		}
		else if (offset==2) {
			codonMut = codon.substring(0, 1)+mutation+codon.substring(2, 3);
		}
		return codonMut;
	}
}
