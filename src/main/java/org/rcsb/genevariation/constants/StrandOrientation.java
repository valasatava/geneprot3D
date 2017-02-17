package org.rcsb.genevariation.constants;

/**
 * An enum defining the orientation of a coding strand
 * 
 * @author Yana Valasatava
 */
public enum StrandOrientation {
	/**
	 * A gene can live on a DNA strand in one of two orientations: forward ("+") and reverse ("-").
	 * 
	 * The gene is said to have a coding strand (also known as its sense strand), 
	 * and a template strand (also known as its antisense strand). For 50% of genes, 
	 * its coding strand will correspond to the chromosome's forward strand, 
	 * and for the other 50% it will correspond to the reverse strand.
	 * 
	*/
	FORWARD, REVERSE
}
