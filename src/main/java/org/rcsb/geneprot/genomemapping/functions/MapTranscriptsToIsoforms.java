package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 10/31/17.
 */
public class MapTranscriptsToIsoforms implements FlatMapFunction<Tuple2<String, Iterable<Row>>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(MapTranscriptsToIsoforms.class);

    private static Map<String, Row> seqMap;
    private static Map<String, Row> varMap;
    public MapTranscriptsToIsoforms(Broadcast<Map<String, Row>> bcSeq, Broadcast<Map<String, Row>> bcVar) {
        seqMap = bcSeq.value();
        varMap = bcVar.value();
    }

    private static boolean translationLength(int transcriptLength){
        if (transcriptLength < 6)
            return false;
        if (transcriptLength % 3 != 0)
            return false;
        return true;
    }

    @Override
    public Iterator<Row> call(Tuple2<String, Iterable<Row>> t) throws Exception {

        String uniProtId = t._1.split(CommonConstants.KEY_SEPARATOR)[3];
        Iterable<Row> it = t._2;

        List<Row> list = new ArrayList<>();
        it.iterator().forEachRemaining(e -> list.add(e));

        try {
            List<String> moleculeIds = new ArrayList<>();
            list.forEach(e -> moleculeIds.add(e.get(e.fieldIndex(CommonConstants.COL_MOLECULE_ID)) != null ?
                    e.getString(e.fieldIndex(CommonConstants.COL_MOLECULE_ID)) : null));

            if ( ! moleculeIds.contains(null) ) { // All transcripts have molecule id associated
                return it.iterator();

            } else { // Transcripts have to be mapped to isoforms

                // =-=-=-=-= BUILDING ISOFORMS =-=-=-=-=

                if (!seqMap.keySet().contains(uniProtId)) {
                    logger.error("Could not retrieve sequence features for {}", uniProtId);
                    return new ArrayList<Row>().iterator();
                }
                Row row = seqMap.get(uniProtId);
                List<Row> sequenceFeatures = row.getList(row.schema().fieldIndex(CommonConstants.COL_FEATURES))
                        .stream().map(e -> (Row) e).collect(Collectors.toList());

                Map<String, String> isoforms;
                try {
                    isoforms = IsoformUtils.buildIsoforms(sequenceFeatures, varMap);
                } catch (Exception e) {
                    logger.error("Cannot build isoform sequences for {}", uniProtId);
                    return new ArrayList<Row>().iterator();
                }

                Map<Integer, List<String>> map = IsoformUtils.mapToLength(isoforms);

                //=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

                List<Row> transcripts = new ArrayList<>();

                for (Row transcript : list ) {

                    if (transcript.get(transcript.fieldIndex(CommonConstants.COL_MOLECULE_ID)) != null) {
                        transcripts.add(transcript);

                    } else {

                        GeneChromosomePosition gcp = IsoformUtils.buildChromosomePosition(transcript);

                        int transcriptLength = ChromosomeMappingTools.getCDSLength(gcp);
                        if (!translationLength(transcriptLength)) {
                            logger.debug("Chromosome positions for {} cannot be translated to protein sequence because of short or not mod3 length"
                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)));
                            continue;
                        }

                        int proteinLength = transcriptLength / 3;
                        if ( !map.keySet().contains(proteinLength)) {
                            logger.info("The sequence of transcript {} doesn't match any isoform sequence of {} entry"
                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION))
                                    , uniProtId);
                            continue;
                        }

                        if( !IsoformUtils.duplicatesIn(map)) {

                            // =-=-=-=-= CHECK BY LENGTH =-=-=-=-=
                            String moleculeId = map.get(proteinLength).get(0);
                            logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION))
                                    , moleculeId);
                            transcripts.add(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId));
                            //=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

                        } else {

                            String sequence;
                            try {
                                sequence = GenomeUtils.getProteinSequence(gcp);
                            } catch (Exception e) {
                                logger.error("Chromosome positions for {} cannot be translated to protein sequence"
                                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)));
                                continue;
                            }

                            // =-=-=-=-= CHECK BY SEQUENCE =-=-=-=-=

                            // Try check equal
                            List<String> ids = map.get(proteinLength);
                            for (String moleculeId : ids) {
                                String isoform = isoforms.get(moleculeId);
                                if (sequence.equals(isoform)) {
                                    logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION))
                                            , moleculeId);
                                    transcripts.add(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId));
                                    continue;
                                }
                            }

                            // Try identity threshold
                            for (String moleculeId : ids) {
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
                                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION))
                                            , moleculeId);
                                    transcripts.add(RowUpdater.updateField(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId), "match", false));
                                    continue;
                                }
                            }
                            //=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

                            logger.info("The sequence of transcript {} doesn't match any isoform sequence length for {} entry"
                                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION))
                                        , uniProtId);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Problem with mapping transcripts to isoforms: {}", e.getMessage());
        }
        return new ArrayList<Row>().iterator();
    }
}