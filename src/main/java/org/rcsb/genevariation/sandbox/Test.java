package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.util.List;

import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;

public class Test {
	
	public static void main(String[] args) throws IOException {
		
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings();
		for (GeneChromosomePosition gcp : gcps) {
			if (gcp.getGeneName().equals("YBX2") && gcp.getGenebankId().equals("NM_015982")) {
				int pos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(7192850, gcp);
				System.out.println(pos);
			}
		}
	}
}
