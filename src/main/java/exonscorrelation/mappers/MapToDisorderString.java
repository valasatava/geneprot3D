package exonscorrelation.mappers;

import exonscorrelation.ExonProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;

public class MapToDisorderString implements MapFunction<ExonProteinFeatures, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3560517396619376413L;

	@Override
	public String call(ExonProteinFeatures feature) throws Exception {
		String line = feature.getChromosome();
		line += ","+feature.getEnsemblId();
		line += ","+String.valueOf(feature.getStart());
		line += ","+String.valueOf(feature.getEnd());
		
		float[] disorder = feature.getDisorder();
		String disorderStr = String.valueOf(disorder[0]);
		for ( int i=1;i< disorder.length; i++ ) {
			disorderStr += ";"+String.valueOf(disorder[i]);
		}
		line += ","+ disorderStr;
		
		return line;
	}

}
