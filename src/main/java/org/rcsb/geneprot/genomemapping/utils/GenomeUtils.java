package org.rcsb.geneprot.genomemapping.utils;

import com.google.common.collect.Range;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.biojava.nbio.genome.util.ProteinMappingTools;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import java.io.File;
import java.util.List;

/**
 * Created by Yana Valasatava on 10/23/17.
 */
public class GenomeUtils {

    private static TwoBitFacade twoBitFacade;

    public static void setGenome(String organism) throws Exception {
        DataLocationProvider.setGenome(organism);
        File f = new File(DataLocationProvider.getGenomeLocation());
        twoBitFacade = new TwoBitFacade(f);
    }

    public static String getTranscriptSequence(GeneChromosomePosition gcp) throws Exception
    {
        List<Range<Integer>> cdsRegion = ChromosomeMappingTools
                .getCDSRegions(gcp.getExonStarts(), gcp.getExonEnds(), gcp.getCdsStart(), gcp.getCdsEnd());

        String dnaSequence = "";
        for (Range<Integer> range : cdsRegion) {
            String exonSequence = twoBitFacade.getSequence(gcp.getChromosome(), range.lowerEndpoint(), range.upperEndpoint());
            dnaSequence += exonSequence;
        }
        return dnaSequence;
    }

    public static String getProteinSequence(GeneChromosomePosition gcp) throws Exception {

        String transcriptSequence = getTranscriptSequence(gcp);
        ProteinSequence sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptSequence);
        return sequence.getSequenceAsString();
    }

    public static void main(String[] args) {
    }
}
