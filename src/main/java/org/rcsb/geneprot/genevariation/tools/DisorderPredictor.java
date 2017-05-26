package org.rcsb.geneprot.genevariation.tools;

import org.biojava.nbio.data.sequence.FastaSequence;
import org.biojava.nbio.ronn.Jronn;

import java.io.Serializable;

public class DisorderPredictor implements Serializable {

	/** Calculate the probability value for each residue in the protein sequence to belong
	 * to disordered region. Biojava implementation of RONN is used.
	 * In general, values greater than 0.5 considered to be in the disordered regions.
	 */
	private static final long serialVersionUID = 8982451308505358723L;

	/** Runs RONN biojava disorder predictor on the protein sequence.
	 *
	 * @param sequence the protein sequence
	 * @return the array of the probability values for each residue in the protein sequence to belong
	 * to disordered region
	 */
	public static float[] run(String sequence) {
		FastaSequence fsequence = new FastaSequence("", sequence);
		return run(fsequence);
	}

	/** Runs RONN biojava disorder predictor on the protein sequence.
	 *
	 * @param fsequence the protein {@link org.biojava.nbio.data.sequence.FastaSequence} FastaSequence
	 * @return the array of the probability values for each residue in the protein sequence to belong
	 * to disordered region
	 */
	public static float[] run(FastaSequence fsequence) {
		float[] rawProbabilityScores = Jronn.getDisorderScores(fsequence);
		return rawProbabilityScores;
	}
}