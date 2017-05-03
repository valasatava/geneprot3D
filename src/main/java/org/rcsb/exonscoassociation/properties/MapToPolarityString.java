package org.rcsb.exonscoassociation.properties;

import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;

public class MapToPolarityString implements MapFunction<ProteinFeatures, String> {

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
		
		int[] prop = feature.getPolarity();
		if ( prop.length==0 )
			return line += ",";

		String str = String.valueOf(prop[0]);
		for ( int i=1;i< prop.length; i++ ) {
			str += ";"+String.valueOf(prop[i]);
		}
		line += ","+ str;
		
		return line;
	}

}
