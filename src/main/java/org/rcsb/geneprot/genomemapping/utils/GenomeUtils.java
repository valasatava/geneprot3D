package org.rcsb.geneprot.genomemapping.utils;

import com.google.common.collect.Range;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.exceptions.TranslationException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ProteinMappingTools;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by Yana Valasatava on 10/23/17.
 */
public class GenomeUtils {

    private static final Logger logger = LoggerFactory.getLogger(GenomeUtils.class);

    public static void setGenome(String organism) throws Exception {
        DataLocationProvider.setGenome(organism);
    }

    public static String getProteinSequence(String orientation, String transcriptSequence) throws CompoundNotFoundException, TranslationException {

        if (orientation.equals('-')) {
            transcriptSequence = (new StringBuilder(transcriptSequence)).reverse().toString();
            DNASequence dna = new DNASequence(transcriptSequence);
            SequenceView<NucleotideCompound> compliment = dna.getComplement();
            transcriptSequence = compliment.getSequenceAsString();
        }
        ProteinSequence sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptSequence);
        return sequence.getSequenceAsString();
    }

    public static String getTranscriptSequence(String chr, List<Range<Integer>> cds) throws Exception
    {
        File f = new File(DataLocationProvider.getGenomeLocation());
        TwoBitFacade twoBitFacade = new TwoBitFacade(f);

        String dnaSequence = "";
        for (Range<Integer> range : cds) {
            String exonSequence = twoBitFacade.getSequence(chr, range.lowerEndpoint(), range.upperEndpoint());
            dnaSequence += exonSequence;
        }
        return dnaSequence;
    }
}
