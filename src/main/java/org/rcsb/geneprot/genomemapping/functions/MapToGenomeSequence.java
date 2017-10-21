package org.rcsb.geneprot.genomemapping.functions;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class MapToGenomeSequence implements Function<GeneChromosomePosition, String>, Serializable {

    private static TwoBitFacade twoBitFacade;
    public MapToGenomeSequence(String organism) throws Exception
    {
        DataLocationProvider.setGenome(organism);
        File f = new File(DataLocationProvider.getGenomeLocation());
        twoBitFacade = new TwoBitFacade(f);
    }

    @Override
    public String call(GeneChromosomePosition gcp) throws Exception
    {
        List<Range<Integer>> cdsRegion = ChromosomeMappingTools
                .getCDSRegions(gcp.getExonStarts(), gcp.getExonEnds(), gcp.getCdsStart(), gcp.getCdsEnd());

        String dnaSequence = "";
        for (Range<Integer> range : cdsRegion) {
            String exonSequence = twoBitFacade.getSequence(gcp.getChromosome(), range.upperEndpoint(), range.lowerEndpoint());
            dnaSequence += exonSequence;
        }
        return dnaSequence;
    }
}
