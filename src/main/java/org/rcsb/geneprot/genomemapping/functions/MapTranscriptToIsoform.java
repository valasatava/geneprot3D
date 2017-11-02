package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.utils.GenomeUtils;
import org.rcsb.geneprot.genomemapping.utils.IsoformUtils;
import org.rcsb.geneprot.genomemapping.utils.RowUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class MapTranscriptToIsoform implements Function<Tuple2<Row, GeneChromosomePosition>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(MapTranscriptToIsoform.class);

    private static String organism;
    private static Map<String, Row> seqMap;
    private static Map<String, Row> varMap;

    public MapTranscriptToIsoform(String organismName, Broadcast<Map<String, Row>> bcSeq, Broadcast<Map<String, Row>> bcVar) throws Exception
    {
        organism = organismName;
        seqMap = bcSeq.value();
        varMap = bcVar.value();
    }

    @Override
    public Row call(Tuple2<Row, GeneChromosomePosition> t) throws Exception
    {
        Row transcript = t._1;
        String uniProtId = transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION));

        if ( ! seqMap.keySet().contains(uniProtId) ) {
            logger.error("Could not retrieve sequence features for {}", uniProtId);
            return null;
        }

        Row row = seqMap.get(uniProtId);
        List<Row> sequenceFeatures = row.getList(row.schema().fieldIndex(CommonConstants.COL_FEATURES))
                .stream().map(e->(Row)e).collect(Collectors.toList());

        Map<String, String> isoforms;
        try {
            isoforms = IsoformUtils.buildIsoforms(sequenceFeatures, varMap);
        } catch (Exception e) {
            logger.error("Cannot build isoforms for {}", uniProtId);
            return null;
        }

        GeneChromosomePosition gcp = t._2;
        int transcriptLength = ChromosomeMappingTools.getCDSLength(gcp);
        if (transcriptLength < 6) {
            logger.info("Chromosome positions for {} cannot be translated to protein sequence because of short transcript length"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
            return null;
        }

        int proteinLength = transcriptLength / 3;

        Map<Integer, List<String>> map = IsoformUtils.mapToLength(isoforms);

        GenomeUtils.setGenome(organism);

        if (! map.keySet().contains(proteinLength)) {

            String s;
            try {
                s = GenomeUtils.getProteinSequence(gcp);
            } catch (Exception e) {
                logger.error("Chromosome positions for {} cannot be translated to protein sequence"
                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
                return null;
            }

            for (String moleculeId : isoforms.keySet()) {
                if (isoforms.get(moleculeId).contains(s) || s.contains(isoforms.get(moleculeId))) {
                    logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                            , moleculeId);
                    return RowUpdater.updateField(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId), CommonConstants.COL_MATCH, false);
                }
            }

            logger.info("The sequence of transcript {} doesn't match any isoform sequence of {} entry"
                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                    , uniProtId);
            return null;
        }

        if(IsoformUtils.duplicatesIn(map)) {

            String sequence;
            try {

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
                    return RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId);
                }
            }

            // Try identity threshold
            for (String moleculeId : moleculeIds) {
                String isoform = isoforms.get(moleculeId);
                int count = 0;
                try {
                    count = IsoformUtils.difference(sequence, isoform);
                } catch (Exception e) {
                    logger.error("Error occurred at {}: gettitng the difference between sequence of length {} and isoform of length {}"
                            , uniProtId, sequence.length(), isoform.length());
                }
                float identity =(float) (sequence.length() - count) / sequence.length();
                if ( identity > 0.99f) {
                    logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                            , moleculeId);
                    return RowUpdater.updateField(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId), "match", false);
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
            return RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId);
        }
    }
}