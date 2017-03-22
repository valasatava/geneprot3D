package exonscorrelation;

import org.apache.spark.api.java.function.MapFunction;

public class MapExonsToDNA implements MapFunction<ExonData, ExonData> {

	private static final long serialVersionUID = -8408800919268086242L;

	@Override
	public ExonData call(ExonData exon) throws Exception {
		
		
		
		return exon;
	}

}
