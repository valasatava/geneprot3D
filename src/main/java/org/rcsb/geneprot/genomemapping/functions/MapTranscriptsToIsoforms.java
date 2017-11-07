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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 10/31/17.
 */
public class MapTranscriptsToIsoforms implements FlatMapFunction<Iterable<Row>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(MapTranscriptsToIsoforms.class);

    private static String organism;
    private static Map<String, Row> seqMap;
    private static Map<String, Row> varMap;

    public MapTranscriptsToIsoforms(String organismName, Broadcast<Map<String, Row>> bcSeq, Broadcast<Map<String, Row>> bcVar) throws Exception
    {
        organism = organismName;
        seqMap = bcSeq.value();
        varMap = bcVar.value();
    }

    @Override
    public Iterator<Row> call(Iterable<Row> it) throws Exception {

        List<Row> list = new ArrayList<>();
        List<Row> transcripts = null;
        try {
            it.iterator().forEachRemaining(e -> list.add(e));

            List<String> moleculeIds = new ArrayList<>();
            list.forEach(e -> moleculeIds.add(e.get(e.fieldIndex(CommonConstants.COL_MOLECULE_ID)) != null ?
                    e.getString(e.fieldIndex(CommonConstants.COL_MOLECULE_ID)) : null));

            transcripts = new ArrayList<>();
            if ( ! moleculeIds.contains(null) )
                return it.iterator();

            else {
                for (Row transcript : list ) {

                    if (transcript.get(transcript.fieldIndex(CommonConstants.COL_MOLECULE_ID)) != null) {
                        transcripts.add(transcript);

                    } else {

                        GeneChromosomePosition gcp = IsoformUtils.buildChromosomePosition(transcript);
                        int transcriptLength = ChromosomeMappingTools.getCDSLength(gcp);
                        if (transcriptLength < 6) {
                            logger.info("Chromosome positions for {} cannot be translated to protein sequence because of short transcript length"
                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
                            continue;
                        }
                        //TODO check the division
                        int proteinLength = transcriptLength / 3;

                        String uniProtId = transcript.getString(transcript.schema().fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION));
                        if (!seqMap.keySet().contains(uniProtId)) {
                            logger.error("Could not retrieve sequence features for {}", uniProtId);
                            continue;
                        }

                        Row row = seqMap.get(uniProtId);
                        List<Row> sequenceFeatures = row.getList(row.schema().fieldIndex(CommonConstants.COL_FEATURES))
                                    .stream().map(e -> (Row) e).collect(Collectors.toList());

                        Map<String, String> isoforms;
                        try {
                            isoforms = IsoformUtils.buildIsoforms(sequenceFeatures, varMap);
                        } catch (Exception e) {
                            logger.info("Cannot build isoforms for {}", uniProtId);
                            continue;
                        }

                        Map<Integer, List<String>> map = IsoformUtils.mapToLength(isoforms);
                        if (map.keySet().contains(proteinLength)) {

                            if(IsoformUtils.duplicatesIn(map)) {

                                String sequence;
                                try {
                                    sequence = GenomeUtils.getProteinSequence(gcp);
                                } catch (Exception e) {
                                    logger.error("Chromosome positions for {} cannot be translated to protein sequence"
                                            , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
                                    continue;
                                }

                                // Try check equal
                                List<String> ids = map.get(proteinLength);
                                for (String moleculeId : ids) {
                                    String isoform = isoforms.get(moleculeId);
                                    if (sequence.equals(isoform)) {
                                        logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                                                , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
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
                                                , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                                                , moleculeId);
                                        transcripts.add(RowUpdater.updateField(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId), "match", false));
                                        continue;
                                    }
                                }

                                logger.info("The sequence of transcript {} doesn't match any isoform sequence length for {} entry"
                                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                                        , uniProtId);
                                continue;

                            } else {
                                String moleculeId = map.get(proteinLength).get(0);
                                logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
                                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                                        , moleculeId);
                                transcripts.add(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId));
                                continue;
                            }
                        } else {

    //                        String sequence;
    //                        try {
    //                            sequence = GenomeUtils.getProteinSequence(gcp);
    //                        } catch (Exception e) {
    //                            logger.error("Chromosome positions for {} cannot be translated to protein sequence"
    //                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
    //                            continue;
    //                        }
    //
    //                        for (String moleculeId : isoforms.keySet()) {
    //                            if (isoforms.get(moleculeId).contains(sequence) || sequence.contains(isoforms.get(moleculeId))) {
    //                                logger.info("The sequence of transcript {} is mapped to isoform sequence {}"
    //                                        , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
    //                                        , moleculeId);
    //                                transcripts.add(RowUpdater.updateField(RowUpdater.updateField(transcript, CommonConstants.COL_MOLECULE_ID, moleculeId), CommonConstants.COL_MATCH, false));
    //                                continue;
    //                            }
    //                        }

                            logger.info("The sequence of transcript {} doesn't match any isoform sequence of {} entry"
                                    , transcript.getString(transcript.schema().fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                                    , uniProtId);
                            continue;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Problem with mapping transcripts to isoforms: {}", e.getMessage());
        }

        return transcripts.iterator();
    }
}