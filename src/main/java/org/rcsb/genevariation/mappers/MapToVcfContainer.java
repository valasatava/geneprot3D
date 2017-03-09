package org.rcsb.genevariation.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.datastructures.VcfContainer;

public class MapToVcfContainer implements FlatMapFunction<Row,VcfContainer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1796199023220653897L;

	@Override
	public Iterator<VcfContainer> call(Row arg0) throws Exception {
		
		List<VcfContainer> vcfData = new ArrayList<VcfContainer>();
		for (String variation : arg0.getString(4).split(",")) {
			VcfContainer v = new VcfContainer();
			v.setChromosome(arg0.getString(0));
			v.setPosition(Integer.valueOf(arg0.getString(1)));
			v.setDbSnpID(arg0.getString(2));
			v.setOriginal(arg0.getString(3));
			v.setVariant(variation);						
			vcfData.add(v);
		}
		return vcfData.iterator();
	}
}
