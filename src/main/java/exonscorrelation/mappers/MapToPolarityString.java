package exonscorrelation.mappers;

import exonscorrelation.ExonProteinFeatures;
import org.apache.spark.api.java.function.MapFunction;

public class MapToPolarityString implements MapFunction<ExonProteinFeatures, String> {

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
		
		int[] prop = feature.getPolarity();
		String str = String.valueOf(prop[0]);
		for ( int i=1;i< prop.length; i++ ) {
			str += ";"+String.valueOf(prop[i]);
		}
		line += ","+ str;
		
		return line;
	}

}
