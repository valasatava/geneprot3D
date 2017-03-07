package org.rcsb.genevariation.mappers;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.datastructures.VcfContainer;

public class FilterCodingRegion implements FilterFunction<VcfContainer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3383886556102006905L;
	
	Broadcast<List<GeneChromosomePosition>> chromosomePositions;
	public FilterCodingRegion(Broadcast<List<GeneChromosomePosition>> val) {
		this.chromosomePositions = val;
	}

	@Override
	public boolean call(VcfContainer vcfDatum) throws Exception {
		
		List<GeneChromosomePosition> gcps = chromosomePositions.getValue();
		for (GeneChromosomePosition cp : gcps) {
			if (cp.getChromosome().equals(vcfDatum.getChromosome())) {
				int pos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(vcfDatum.getPosition(), cp);
				if ( pos != -1 )
					return true;
			}
		}
		return false;
	}
}
