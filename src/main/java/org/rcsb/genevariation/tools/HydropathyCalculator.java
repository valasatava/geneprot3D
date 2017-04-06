package org.rcsb.genevariation.tools;

import org.biojava.nbio.aaproperties.PeptideProperties;

import java.io.Serializable;

public class HydropathyCalculator implements Serializable {

	/** Calculate an average hydropathy value by deviding the sum of hydropathy values
     *  within a sliding window of 15 amino acids by the number of residues in the window.
     *
     *  Hydropathy values are based on (Kyte, J. and Doolittle, R.F. (1982)
     *  A simple method for displaying the hydropathic character of a protein. J. Mol. Biol. 157, 105-132).
	 */
	private static final long serialVersionUID = -2163065969425788857L;

	static final int windowSize = 15;

    /** Runs biojava hydropathy calculator on the protein sequence.
     *
     * @param sequence the protein sequence
     * @return the array of the hydropathy values for each residue in the protein sequence
     */
	public static float[] run(String sequence) {

		float[] hydropathy = new float[sequence.length()];

		for ( int i = 0 ; i< sequence.length() ; i ++) {

			int halfWin = windowSize / 2;
            int rightWin = halfWin;
            int leftWin = halfWin;

            if (i + rightWin >= sequence.length()) {
               rightWin = sequence.length() - i;
            }
            if (i - leftWin < 0) {
               leftWin = i;
            }

            String sub = sequence.substring(i - leftWin, i + rightWin);
            double hydrop = PeptideProperties.getAvgHydropathy(sub);
            hydropathy[i] = (float) hydrop;
         }
		return hydropathy;
	}
}
