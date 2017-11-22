package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.TranscriptToIsoform;
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
        coordinates.setId(r.getInt(r.fieldIndex(CommonConstants.COL_ID)));
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

        for (Row annotation : transcripts)
        {
            TranscriptToIsoform t = new TranscriptToIsoform();

            t.setTranscriptId(annotation.getString(annotation.fieldIndex(CommonConstants.COL_TRANSCRIPT_ID)));
            t.setTranscriptName(annotation.getString(annotation.fieldIndex(CommonConstants.COL_TRANSCRIPT_NAME)));

            for (Object o : annotation.getList(annotation.fieldIndex(CommonConstants.COL_CODING))) {
                t.getCodingCoordinates().add(getCoordinates((Row)o));
            }

            t.setMoleculeId(annotation.getString(annotation.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
            t.setSequence(annotation.getString(annotation.fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE)));
            t.setSequenceStatus(annotation.getString(annotation.fieldIndex(CommonConstants.COL_SEQUENCE_STATUS)));

            int mRNAPosEnd;
            int mRNAPosStart = 0;
            for (CoordinatesRange cds : t.getCodingCoordinates()) {
                mRNAPosEnd = mRNAPosStart + cds.getEnd()-cds.getStart();
                t.getmRNACoordinates().add(new CoordinatesRange(cds.getId(), mRNAPosStart, mRNAPosEnd));
                t.getIsoformCoordinates().add(new CoordinatesRange(cds.getId(), (int) Math.ceil(mRNAPosStart / 3.0f) + 1
                        , (int)Math.ceil(mRNAPosEnd/3.0f)));
                mRNAPosStart = mRNAPosEnd+1;
            }

            m.getIsoforms().add(t);
        }
        return m;
    }
}