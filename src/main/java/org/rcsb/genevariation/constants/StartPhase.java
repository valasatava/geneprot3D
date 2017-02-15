package org.rcsb.genevariation.constants;

/**
 * An enum defining the phase of variant
 * 
 * @author Yana Valasatava
 */
public enum StartPhase {
	/**
	 * Phase the start phase is the place where the intron lands inside the codon : 
	 * 0 between codons, 
	 * 1 between the 1st and second base, 
	 * 2 between the second and 3rd base.
	 */ 
	PHASE_ZERO(0),
	PHASE_ONE(1),
	PHASE_TWO(2);
	
	private final int value;

	StartPhase(final int val) {
        value = val;
    }
    public int getValue() { return value; }
}
