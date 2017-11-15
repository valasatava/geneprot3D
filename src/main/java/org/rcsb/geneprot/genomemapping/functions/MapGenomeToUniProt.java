package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.model.TranscriptMapping;
import org.rcsb.uniprot.isoform.IsoformMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class MapGenomeToUniProt implements Function<Row, GenomeToUniProtMapping> {

    private static final Logger logger = LoggerFactory.getLogger(MapGenomeToUniProt.class);

    private static CoordinatesRange getCoordinates(Row r) {

        CoordinatesRange coordinates = new CoordinatesRange();
        coordinates.setStart(r.getInt(r.fieldIndex(CommonConstants.COL_START)));
        coordinates.setEnd(r.getInt(r.fieldIndex(CommonConstants.COL_END)));
        return coordinates;
    }

    @Override
    public GenomeToUniProtMapping call(Row row) throws Exception {

        GenomeToUniProtMapping m = new GenomeToUniProtMapping();

        m.setChromosome(row.getString(row.fieldIndex(CommonConstants.COL_CHROMOSOME)));
        m.setGeneId(row.getString(row.fieldIndex(CommonConstants.COL_GENE_ID)));
        m.setGeneName(row.getString(row.fieldIndex(CommonConstants.COL_GENE_NAME)));
        m.setOrientation(row.getString(row.fieldIndex(CommonConstants.COL_ORIENTATION)));
        m.setUniProtId(row.getString(row.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)));

        List<Row> annotations = row.getList(row.fieldIndex(CommonConstants.COL_TRANSCRIPTS));

        Row can = annotations.stream()
                .filter(r -> r.getString(r.fieldIndex(CommonConstants.COL_SEQUENCE_STATUS)).equals("displayed"))
                .collect(Collectors.toList()).get(0);
        String canonical = can.getString(can.fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE));
        ProteinSequence canonicalSequence = new ProteinSequence(canonical);

        for (Row annotation : annotations)
        {
            TranscriptMapping t = new TranscriptMapping();

            t.setTranscriptId(annotation.getString(annotation.fieldIndex(CommonConstants.COL_TRANSCRIPT_ID)));
            t.setTranscriptName(annotation.getString(annotation.fieldIndex(CommonConstants.COL_TRANSCRIPT_NAME)));

            Row r = annotation.getStruct(annotation.fieldIndex(CommonConstants.COL_TRANSCRIPTION));
            t.setTranscriptionCoordinates(getCoordinates(r));

            for (Object o : annotation.getList(annotation.fieldIndex(CommonConstants.COL_UTR))) {
                t.getUrtCoordinates().add(getCoordinates((Row)o));
            }

            t.setExonsCount(annotation.getInt(annotation.fieldIndex(CommonConstants.COL_EXONS_COUNT)));
            for (Object o : annotation.getList(annotation.fieldIndex(CommonConstants.COL_EXONS))) {
                t.getExonCoordinates().add(getCoordinates((Row)o));
            }

            for (Object o : annotation.getList(annotation.fieldIndex(CommonConstants.COL_CODING))) {
                t.getCodingCoordinates().add(getCoordinates((Row)o));
            }

            t.setMoleculeId(annotation.getString(annotation.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
            t.setSequence(annotation.getString(annotation.fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE)));
            t.setSequenceStatus(annotation.getString(annotation.fieldIndex(CommonConstants.COL_SEQUENCE_STATUS)));

            ProteinSequence isoformSequence = new ProteinSequence(t.getSequence());
            IsoformMapper isomapper = new IsoformMapper(canonicalSequence, isoformSequence);

            int mRNAPosEnd;
            int mRNAPosStart = 0;
            for (CoordinatesRange cds : t.getCodingCoordinates())
            {
                mRNAPosEnd = mRNAPosStart + cds.getEnd()-cds.getStart();
                t.getmRNACoordinates().add(new CoordinatesRange(mRNAPosStart, mRNAPosEnd));
                int c1 = (int) Math.ceil(mRNAPosStart / 3.0f) + 1;
                int c2 = (int)Math.ceil(mRNAPosEnd/3.0f);
                t.getIsoformCoordinates().add(new CoordinatesRange(c1, c2));
                t.getCanonicalCoordinates().add(new CoordinatesRange(isomapper.convertPos2toPos1(c1), isomapper.convertPos2toPos1(c2)));
                mRNAPosStart = mRNAPosEnd+1;
            }
            t.setHasAlternativeExons(annotation.getBoolean(annotation.fieldIndex(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)));
            t.setAlternativeExons(annotation.getList(annotation.fieldIndex(CommonConstants.COL_ALTERNATIVE_EXONS)));

            m.getTranscripts().add(t);
        }
        return m;
    }
}