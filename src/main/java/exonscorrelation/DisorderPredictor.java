package exonscorrelation;

import org.biojava.nbio.data.sequence.FastaSequence;
import org.biojava.nbio.ronn.Jronn;

import java.io.Serializable;

public class DisorderPredictor implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 8982451308505358723L;

	public static float[] run(String sequence) {
		FastaSequence fsequence = new FastaSequence("", sequence);
		float[] rawProbabilityScores = Jronn.getDisorderScores(fsequence);
		return rawProbabilityScores;
	}
}
