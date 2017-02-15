package org.rcsb.genevariation.constants;

/**
 * An enum defining the types of variants reported in VCF file
 * 
 * @author Yana Valasatava
 */
public enum VariantType {
	
	/**
	 * Lists an array of Strings to define the type of variation reported in VCF file.
	 * 
	 * The variants can be of the following types:
	 * - a simple SNP (e.g. A -> T)
	 * - a site that is called monomorphic reference (i.e. with no alternate alleles) (e.g. A -> .)
	 * - a microsatellite site with two alternative alleles (GTC -> G, GTCT):
	 * -- deletion:  GTC -> G
	 * -- insertion: GTC -> GTCT
	 */
	SNP, MONOMORPHIC, INSERTION, DELETION
}
