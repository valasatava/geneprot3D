package org.rcsb.geneprot.transcriptomics.properties;

import org.rcsb.geneprot.common.datastructures.ProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;

public class MapToChargesString implements MapFunction<ProteinFeatures, String> {

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
		
		int[] charges = feature.getCharge();
		if ( charges.length==0 )
			return line += ",";

		String disorderStr = String.valueOf(charges[0]);
		for ( int i=1;i< charges.length; i++ ) {
			disorderStr += ";"+String.valueOf(charges[i]);
		}
		line += ","+ disorderStr;
		
		return line;
	}

}
