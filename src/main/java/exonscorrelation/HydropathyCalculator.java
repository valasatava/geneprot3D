package exonscorrelation;

import org.biojava.nbio.aaproperties.PeptideProperties;

import java.io.Serializable;

public class HydropathyCalculator implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -2163065969425788857L;

	static final int windowSize = 15;

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
