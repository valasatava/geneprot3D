package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.utils.GenomeUtils;
import org.rcsb.geneprot.genomemapping.utils.IsoformUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class MapTranscriptToIsoform implements Function<Tuple2<Row, GeneChromosomePosition>, Tuple3<Row, String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(MapTranscriptToIsoform.class);

    private Map<Integer, List<String>> mapToLength(Map<String, String> isoforms)
    {
        Map<Integer, List<String>> lengthMap = new HashMap<>();
        for (String moleculeId : isoforms.keySet())
        {
            int sequenceLenght = isoforms.get(moleculeId).length();
            if (! lengthMap.keySet().contains(sequenceLenght))
                lengthMap.put(sequenceLenght, new ArrayList<>());
            lengthMap.get(sequenceLenght).add(moleculeId);
        }
        return lengthMap;
    }

    private boolean duplicatesIn(Map<Integer, List<String>> lengthMap)
    {
        for (List<String> values : lengthMap.values()){
            if (values.size()>1)
                return true;
        }
        return false;
    }

    private int difference(String one, String two)
    {
        char[] first  = one.toCharArray();
        char[] second = two.toCharArray();
        int count=0;
        for (int i = 0; i < first.length; i++) {
            if(first[i] != second[i])
                count++;
        }
        return count;
    }

    private static String organism;

    public MapTranscriptToIsoform(String organism) throws Exception {
        this.organism = organism;
    }

    @Override
    public Tuple3<Row, String, String> call(Tuple2<Row, GeneChromosomePosition> t) throws Exception {

        Row transcript = t._1;
        String uniProtId = transcript.getString(transcript.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION));

        Map<String, String> isoforms;
        try {
            isoforms = IsoformUtils.buildIsoforms(uniProtId);
        } catch (Exception e) {
            logger.error("Cannot build isoforms for {}", uniProtId);
            return null;
        }

        if (isoforms == null) {
            logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                    , uniProtId+"-1");
            return new Tuple3<>(transcript, CommonConstants.MOLECULE_ID, uniProtId+"-1");
        }

        GeneChromosomePosition gcp = t._2;
        int transcriptLength = ChromosomeMappingTools.getCDSLength(gcp);
        if (transcriptLength < 6) {
            logger.error("Chromosome positions for {} cannot be translated to protein sequence because of short transcript length"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
            return null;
        }

        int proteinLength = transcriptLength / 3;

        Map<Integer, List<String>> map = mapToLength(isoforms);
        if (! map.keySet().contains(proteinLength)) {
            logger.info("The sequence of transcript {} doesn't match any isoform sequence length for {} entry"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                    , uniProtId);
            return null;
        }

        if(duplicatesIn(map)) {

            String sequence;
            try {
                GenomeUtils.setGenome(organism);
                sequence = GenomeUtils.getProteinSequence(gcp);
            } catch (Exception e) {
                logger.error("Chromosome positions for {} cannot be translated to protein sequence"
                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
                return null;
            }

            // Try check equal
            List<String> moleculeIds = map.get(proteinLength);
            for (String moleculeId : moleculeIds) {
                String isoform = isoforms.get(moleculeId);
                if (sequence.equals(isoform)) {
                    logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                            , moleculeId);
                    return new Tuple3<>(transcript, CommonConstants.MOLECULE_ID, moleculeId);
                }
            }

            // Try identity threshold
            for (String moleculeId : moleculeIds) {
                String isoform = isoforms.get(moleculeId);
                int count = 0;
                try {
                    count = difference(sequence, isoform);
                } catch (Exception e) {
                    logger.error("Error occurred at {}: gettitng the difference between sequence of length {} and isoform of length {}"
                            , uniProtId, sequence.length(), isoform.length());
                }
                float identity =(float) (sequence.length() - count) / sequence.length();
                if ( identity > 0.99f) {
                    logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                            , moleculeId);
                    return new Tuple3<>(transcript, CommonConstants.MOLECULE_ID, moleculeId);
                }
            }

            logger.info("The sequence of transcript {} doesn't match any isoform sequence length for {} entry"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                    , uniProtId);
            return null;

        } else {
            String moleculeId = map.get(proteinLength).get(0);
            logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                    , moleculeId);
            return new Tuple3<>(transcript, CommonConstants.MOLECULE_ID, moleculeId);
        }
    }
}