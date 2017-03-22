package exonscorrelation;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import org.biojava.nbio.aaproperties.PeptideProperties;

public class MapToAAPolarity implements MapFunction<Row, ExonProteinFeatures> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6324402511339348107L;

	@Override
	public ExonProteinFeatures call(Row row) throws Exception {
		
		String uniprotIds = row.getString(10);
		int isoformNum = row.getInt(9);
			
		String uniprotId;
		if (uniprotIds.contains(",")) {
			System.out.println(uniprotIds);
			uniprotId = uniprotIds.split(",")[0];
		}
		else {
			uniprotId = uniprotIds;
		}
		
		String isoform = UniprotMapping.getIsoform(uniprotId, isoformNum);

		int isoformStart;
		int isoformEnd;

		String orientation = row.getString(7);
		if (orientation.equals("+")) {
			isoformStart = row.getInt(0);
			isoformEnd = row.getInt(11);
		}
		else {
			isoformStart = row.getInt(11);
			isoformEnd = row.getInt(0);
		}
		
		if (isoformStart == -1 || isoformEnd == -1)
			return null;
		
		String peptide = isoform.subSequence(isoformStart-1, isoformEnd).toString();
		int[] polarity = PeptideProperties.getPolarityOfAminoAcids(peptide);
		
		ExonProteinFeatures feature = new ExonProteinFeatures();
		feature.setChromosome(row.getString(2));
		feature.setEnsemblId(row.getString(4));
		feature.setStart(row.getInt(8));
		feature.setEnd(row.getInt(3));
		feature.setPolarity(polarity);
		
		return feature;
	}
}
