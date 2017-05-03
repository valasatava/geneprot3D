package org.rcsb.exonscoassociation.properties;

import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;

public class MapToDisorderString implements MapFunction<ProteinFeatures, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3560517396619376413L;

	@Override
	public String call(ProteinFeatures feature) throws Exception {

		String line = feature.getChromosome();
		line += ","+feature.getEnsemblId();
		line += ","+String.valueOf(feature.getStart());
		line += ","+String.valueOf(feature.getEnd());

		float[] disorder = feature.getDisorder();
		if ( disorder.length==0 )
			return line += ",";

		String disorderStr = String.valueOf(disorder[0]);
		for ( int i=1;i< disorder.length; i++ ) {
			disorderStr += ";"+String.valueOf(disorder[i]);
		}
		line += ","+ disorderStr;
		
		return line;
	}

}
