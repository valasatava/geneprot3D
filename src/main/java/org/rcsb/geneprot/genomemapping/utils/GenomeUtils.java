package org.rcsb.geneprot.genomemapping.utils;

import com.google.common.collect.Range;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
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

    public static void setGenome(String organism) throws Exception {
        DataLocationProvider.setGenome(organism);
    }

    public static String getTranscriptSequence(GeneChromosomePosition gcp) throws Exception
    {
        File f = new File(DataLocationProvider.getGenomeLocation());
        TwoBitFacade twoBitFacade = new TwoBitFacade(f);

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

        if (gcp.getOrientation().equals('-')) {
            transcriptSequence = (new StringBuilder(transcriptSequence)).reverse().toString();
            DNASequence dna = new DNASequence(transcriptSequence);
            SequenceView<NucleotideCompound> compliment = dna.getComplement();
            transcriptSequence = compliment.getSequenceAsString();
        }

        ProteinSequence sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptSequence);
        return sequence.getSequenceAsString();
    }

    public static void main(String[] args) {
    }
}
