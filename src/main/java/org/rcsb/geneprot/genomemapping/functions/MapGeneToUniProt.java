package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.TranscriptToUniProt;
import org.rcsb.geneprot.genomemapping.model.GeneToUniProt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class MapGeneToUniProt implements Function<Row, GeneToUniProt> {

    private static final Logger logger = LoggerFactory.getLogger(MapGeneToUniProt.class);

    private static CoordinatesRange getCoordinates(Row r) {

        CoordinatesRange coordinates = new CoordinatesRange();
        coordinates.setStart(r.getInt(r.fieldIndex(CommonConstants.COL_START)));
        coordinates.setEnd(r.getInt(r.fieldIndex(CommonConstants.COL_END)));
        return coordinates;
    }

    @Override
    public GeneToUniProt call(Row row) throws Exception {

        GeneToUniProt m = new GeneToUniProt();

        m.setChromosome(row.getString(row.fieldIndex(CommonConstants.COL_CHROMOSOME)));
        m.setGeneId(row.getString(row.fieldIndex(CommonConstants.COL_GENE_ID)));
        m.setGeneName(row.getString(row.fieldIndex(CommonConstants.COL_GENE_NAME)));
        m.setOrientation(row.getString(row.fieldIndex(CommonConstants.COL_ORIENTATION)));
        m.setUniProtId(row.getString(row.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)));

        List<Row> transcripts = row.getList(row.fieldIndex(CommonConstants.COL_TRANSCRIPTS));

//        if (transcripts == null || transcripts.size() == 0)
//            return null;
//
//        String canonical = transcripts.get(0).getString(transcripts.get(0).fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE));
//        for (Row r : transcripts) {
//            if (r.get(r.fieldIndex(CommonConstants.COL_SEQUENCE_STATUS))!=null && r.getString(r.fieldIndex(CommonConstants.COL_SEQUENCE_STATUS)).equals("displayed")) {
//                canonical = r.getString(r.fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE));
//                break;
//            }
//        }
//
//        ProteinSequence canonicalSequence = new ProteinSequence(canonical);

        for (Row annotation : transcripts)
        {
            TranscriptToUniProt t = new TranscriptToUniProt();

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

//            ProteinSequence isoformSequence = new ProteinSequence(t.getSequence());
//            IsoformMapper isomapper = new IsoformMapper(canonicalSequence, isoformSequence);

            int mRNAPosEnd;
            int mRNAPosStart = 0;
            for (CoordinatesRange cds : t.getCodingCoordinates())
            {
                mRNAPosEnd = mRNAPosStart + cds.getEnd()-cds.getStart();
                t.getmRNACoordinates().add(new CoordinatesRange(mRNAPosStart, mRNAPosEnd));
                int c1 = (int) Math.ceil(mRNAPosStart / 3.0f) + 1;
                int c2 = (int)Math.ceil(mRNAPosEnd/3.0f);
                t.getIsoformCoordinates().add(new CoordinatesRange(c1, c2));
                //t.getCanonicalCoordinates().add(new CoordinatesRange(isomapper.convertPos2toPos1(c1), isomapper.convertPos2toPos1(c2)));
                mRNAPosStart = mRNAPosEnd+1;
            }
            t.setHasAlternativeExons(annotation.getBoolean(annotation.fieldIndex(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)));
            t.setAlternativeExons(annotation.getList(annotation.fieldIndex(CommonConstants.COL_ALTERNATIVE_EXONS)));

            m.getTranscripts().add(t);
        }
        return m;
    }
}