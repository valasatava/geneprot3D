package org.rcsb.geneprot.genes.constants;

/**
 * An enum defining the phase of variant
 * 
 * @author Yana Valasatava
 */
public enum ExonFrameOffset {
	/**
	 * Phase the start phase is the place where the intron lands inside the codon: 
	 * -1 exon falls in UTR fully
	 * 0  between codons, 
	 * 1  between the 1st and second base, 
	 * 2  between the second and 3rd base.
	 */
	PHASE_UTR(-1),
	PHASE_ZERO(0),
	PHASE_ONE(1),
	PHASE_TWO(2);
	
	private final int value;

	ExonFrameOffset(final int val) {
        value = val;
    }
    public int getValue() { return value; }
}
