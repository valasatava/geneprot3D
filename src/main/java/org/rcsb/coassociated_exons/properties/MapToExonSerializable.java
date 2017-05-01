package org.rcsb.coassociated_exons.properties;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.datastructures.ExonSerializable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class MapToExonSerializable implements FlatMapFunction<Row, ExonSerializable> {

	private static final long serialVersionUID = -1871761147320947320L;

	@Override
	public Iterator<ExonSerializable> call(Row row) throws Exception {

		List<ExonSerializable> exons = new ArrayList<ExonSerializable>();

		String chromosome = row.getString(0);

		Integer start = Integer.valueOf(row.getString(1));
		Integer end = Integer.valueOf(row.getString(2));

		String orientation = row.getString(3);

		String geneName = row.getString(4);

		String ensemblIds = row.getString(5);
		String offsets = row.getString(6);

		ExonSerializable exon;
		String ensemblId;
		String offset;

		if (ensemblIds.contains(";")) {

			String[] ids = ensemblIds.split(";");
			String[] offs = offsets.split(";");

			for (int i=0; i<ids.length; i++ ) {

				ensemblId = ids[i];
				offset = offs[i];

				exon = new ExonSerializable(chromosome, geneName, start, end, orientation);
				exon.setEnsemblId(ensemblId);
				exon.setOffset(Integer.valueOf(offset));

				exons.add(exon);
			}
		}
		else {

			ensemblId = row.getString(5).split(Pattern.quote("."))[0];
			offset = row.getString(6);

			exon = new ExonSerializable(chromosome, geneName, start, end, orientation);
			exon.setEnsemblId(ensemblId);
			exon.setOffset(Integer.valueOf(offset));

			exons.add(exon);
		}

		return exons.iterator();
	}
}
