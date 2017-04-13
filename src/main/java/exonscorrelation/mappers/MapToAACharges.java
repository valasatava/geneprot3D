package exonscorrelation.mappers;

import org.rcsb.genevariation.datastructures.ProteinFeatures;
import exonscorrelation.utils.IsoformUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import org.biojava.nbio.aaproperties.PeptideProperties;

import java.util.List;

public class MapToAACharges implements MapFunction<Row, ProteinFeatures> {

	/**
	 *
	 */
	private static final long serialVersionUID = 6324402511339348107L;

	@Override
	public ProteinFeatures call(Row row) throws Exception {

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

		String isoform = IsoformUtils.getIsoform(uniprotId, isoformNum);
		List<Integer> isosten = exonscorrelation.utils.CommonUtils.getIsoStartEndForRow(row);
		int isoformStart = isosten.get(0);
		int isoformEnd = isosten.get(1);

		if (isoformStart == -1 || isoformEnd == -1)
			return null;

		String peptide = isoform.subSequence(isoformStart-1, isoformEnd).toString();
		int[] charge = PeptideProperties.getChargesOfAminoAcids(peptide);

		ProteinFeatures feature = new ProteinFeatures();
		feature.setChromosome(row.getString(2));
		feature.setEnsemblId(row.getString(4));
		feature.setStart(row.getInt(8));
		feature.setEnd(row.getInt(3));
		feature.setCharge(charge);

		return feature;
	}
}
