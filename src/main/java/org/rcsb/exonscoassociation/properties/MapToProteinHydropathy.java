package org.rcsb.exonscoassociation.properties;

import org.rcsb.exonscoassociation.utils.CommonUtils;
import org.rcsb.exonscoassociation.utils.IsoformsUtils;
import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.tools.HydropathyCalculator;

import java.util.List;

public class MapToProteinHydropathy implements MapFunction<Row, ProteinFeatures> {

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

		String isoform = IsoformsUtils.getIsoform(uniprotId, isoformNum);
		List<Integer> isosten = CommonUtils.getIsoStartEndForRow(row);
		int isoformStart = isosten.get(0);
		int isoformEnd = isosten.get(1);

		if (isoformStart == -1 || isoformEnd == -1)
			return null;

		float[] hydropathy = HydropathyCalculator.run(isoform);

		int len = isoformEnd-isoformStart-1;
		float[] hydropathyExon = new float[len];
		System.arraycopy(hydropathy, isoformStart, hydropathyExon, 0, len);

		ProteinFeatures feature = new ProteinFeatures();
		feature.setChromosome(row.getString(2));
		feature.setEnsemblId(row.getString(4));
		feature.setStart(row.getInt(8));
		feature.setEnd(row.getInt(3));
		feature.setHydropathy(hydropathyExon);

		return feature;
	}
}
