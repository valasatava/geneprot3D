package org.rcsb.coassociated_exons.properties;

import org.rcsb.coassociated_exons.utils.CommonUtils;
import org.rcsb.coassociated_exons.utils.IsoformsUtils;
import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.biojava.nbio.aaproperties.PeptideProperties;
import org.rcsb.genevariation.tools.DisorderPredictor;
import org.rcsb.genevariation.tools.HydropathyCalculator;

import java.util.List;

public class MapToProteinFeatures implements MapFunction<Row, ProteinFeatures> {

	/**
	 *
	 */
	private static final long serialVersionUID = -1382222868798031985L;

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

		ProteinFeatures feature = new ProteinFeatures();

		try {
			feature.setChromosome(row.getString(1));
			feature.setEnsemblId(row.getString(3));
			feature.setStart(row.getInt(7));
			feature.setEnd(row.getInt(8));

			String isoform = IsoformsUtils.getIsoform(uniprotId, isoformNum);
			List<Integer> isosten = CommonUtils.getIsoStartEndForRow(row);
			int isoformStart = isosten.get(0);
			int isoformEnd = isosten.get(1);
			if (isoformStart == -1 || isoformEnd == -1)
                return null;

			int len = isoformEnd-isoformStart-1;
			if (len < 0)
				return feature;

			float[] disorder = DisorderPredictor.run(isoform);
			float[] disorderExon = new float[len];
			System.arraycopy(disorder, isoformStart, disorderExon, 0, len);
			feature.setDisorder(disorderExon);

			float[] hydropathy = HydropathyCalculator.run(isoform);
			float[] hydropathyExon = new float[len];
			System.arraycopy(hydropathy, isoformStart, hydropathyExon, 0, len);
			feature.setHydropathy(hydropathyExon);

			String peptide = isoform.subSequence(isoformStart-1, isoformEnd).toString();
			int[] charge = PeptideProperties.getChargesOfAminoAcids(peptide);
			feature.setCharge(charge);

			int[] polarity = PeptideProperties.getPolarityOfAminoAcids(peptide);
			feature.setPolarity(polarity);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return feature;
	}
}
