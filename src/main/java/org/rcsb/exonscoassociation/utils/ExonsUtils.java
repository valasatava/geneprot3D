package org.rcsb.exonscoassociation.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.SequenceView;
import org.rcsb.exonscoassociation.properties.MapToExonSerializable;
import org.rcsb.genevariation.datastructures.ExonSerializable;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by yana on 4/13/17.
 */
public class ExonsUtils {

    public static List<ExonSerializable> getSerializableExons(String dataPath) {

        Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(dataPath);

        Encoder<ExonSerializable> encoder = Encoders.bean(ExonSerializable.class);
        List<ExonSerializable> exons = data
                .flatMap(new MapToExonSerializable(), encoder)
                .collectAsList();

        return exons;
    }

    public static List<ExonSerializable> getUCSCExons() throws IOException {

        List<ExonSerializable> exons = new ArrayList<ExonSerializable>();
        List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings();
        for (Transcript transcript : transcripts) {
            List<Integer> starts = transcript.getExonStarts();
            List<Integer> ends = transcript.getExonEnds();
            for ( int i=0; i<starts.size();i++ ) {
                ExonSerializable exon = new ExonSerializable();
                exon.setChromosome(transcript.getChromosomeName());
                exon.setGeneName(transcript.getGeneName());
                exon.setGeneBankId(transcript.getGeneBankId());
                exon.setStart(starts.get(i)+1);
                exon.setEnd(ends.get(i));
                exons.add(exon);
            }
        }
        return exons;
    }

    public static void getExonsPeptides() throws Exception {

        String chrSet="chr1";
        RNApolymerase polymerase = new RNApolymerase(chrSet);

        Map<String, String> map = new HashMap<String, String>();

        List<ExonSerializable> exons = getSerializableExons("");
        for (ExonSerializable exon : exons) {

            if ( !chrSet.equals(exon.getChromosome())) {
                polymerase.setChromosome(exon.getChromosome());
                chrSet = exon.getChromosome();
            }

            String transcription = "";
            if ( !exon.getOrientation().equals("+") ) {
                int lenght = ((exon.getEnd()-exon.getOffset()) - exon.getStart())+1;
                int correction = lenght%3;
                lenght = lenght-correction;
                transcription = polymerase.parser.loadFragment((exon.getStart()+correction)-1, lenght);
                transcription = new StringBuilder(transcription).reverse().toString();
                DNASequence dna = new DNASequence(transcription);
                SequenceView<NucleotideCompound> compliment = dna.getComplement();
                transcription = compliment.getSequenceAsString();
            }
            else {
                int length = (exon.getEnd() - (exon.getStart()+exon.getOffset()))+1;
                int correction = length%3;
                length = length-correction;
                transcription = polymerase.parser.loadFragment((exon.getStart()+exon.getOffset())-1, length);
            }

            String peptide = Ribosome.getProteinSequence(transcription);
            map.put(exon.getGeneBankId(), peptide);
        }
    }

    public static void sortExons(List<ExonSerializable> exons) {
        //sorting the exons based on the chromosome name
        Collections.sort(exons, new Comparator<ExonSerializable>() {
            @Override
            public int compare(final ExonSerializable e1, final ExonSerializable e2) {
                return e1.getChromosome().compareTo(e2.getChromosome());
            }
        } );
    }



}
