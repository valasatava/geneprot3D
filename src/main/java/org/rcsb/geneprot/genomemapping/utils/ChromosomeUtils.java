package org.rcsb.geneprot.genomemapping.utils;

import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tara on 2/4/16.
 */

public class ChromosomeUtils {

    private static final Logger PdbLogger = LoggerFactory.getLogger(org.rcsb.util.ChromosomeUtils.class);
    public static final int MAX_DNA_LENGTH = 39999;
    public static enum mappingStatus  {UTR, CODING, NONCODING};

    public  Map<Integer, Integer> getExonPositions(GeneChromosomePosition chromosomePosition) {

        Map<Integer, Integer> exonPositions = new HashMap<Integer, Integer>();

        int numExons = chromosomePosition.getExonCount();


        if (numExons > 0) {
            try {
                List<Integer> exonStarts = chromosomePosition.getExonStarts();
                List<Integer> exonEnds = chromosomePosition.getExonEnds();

                for (int j = 0; j < numExons; j++) {
                    int startExonPos = exonStarts.get(j);
                    int endExonPos = exonEnds.get(j);

                    if (startExonPos > 0 && endExonPos > 0 && startExonPos < endExonPos) {
                        for (int k = startExonPos; k <= endExonPos; k++) {
                            //exon numbers start at 1
                            exonPositions.put(k, j + 1);
                        }
                    }
                }
            } catch (Exception e){
                PdbLogger.error("Could not extract exon boundaries from " + chromosomePosition);
                return new HashMap<>();
            }
        }
        return exonPositions;
    }

    public static String format(int chromosomePosition){
        // returns a nicely formatted representation of the position
        return String.format("%,d", chromosomePosition);
    }
}
