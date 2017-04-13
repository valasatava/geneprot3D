package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.datastructures.ExonSerializable;

import java.util.regex.Pattern;

public class MapToExonSerializable implements  MapFunction<Row, ExonSerializable> {

	private static final long serialVersionUID = -1871761147320947320L;

	@Override
	public ExonSerializable call(Row row) throws Exception {

		String chromosome = row.getString(0);
		String orientation = row.getString(3);
		String geneName = row.getString(6);
		Integer start = Integer.valueOf(row.getString(1));
		Integer end = Integer.valueOf(row.getString(2));
		
		String ensemblId = row.getString(5).split(Pattern.quote("."))[0];
		String offset = row.getString(4);

		ExonSerializable exon = new ExonSerializable(chromosome, geneName, start, end, orientation);
		exon.setEnsemblId(ensemblId);
		exon.setOffset(Integer.valueOf(offset));

		return exon;
	}
}
