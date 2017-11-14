package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.model.TranscriptMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class MapGenomeToUniProt implements Function<Row, GenomeToUniProtMapping> {

    private static final Logger logger = LoggerFactory.getLogger(MapGenomeToUniProt.class);

    private static List<Tuple2<Integer, Integer>> getCDSRegions(List<Integer> origExonStarts, List<Integer> origExonEnds, int cdsStart, int cdsEnd)
    {
        List<Integer> exonStarts = new ArrayList(origExonStarts);
        List<Integer> exonEnds = new ArrayList(origExonEnds);
        int j = 0;

        int nExons;
        for(nExons = 0; nExons < origExonStarts.size(); ++nExons) {
            if((origExonEnds.get(nExons)).intValue() >= cdsStart && (origExonStarts.get(nExons)).intValue() <= cdsEnd) {
                ++j;
            } else {
                exonStarts.remove(j);
                exonEnds.remove(j);
            }
        }

        nExons = exonStarts.size();
        exonStarts.remove(0);
        exonStarts.add(0, Integer.valueOf(cdsStart));
        exonEnds.remove(nExons - 1);
        exonEnds.add(Integer.valueOf(cdsEnd));

        List<Tuple2<Integer, Integer>> cdsRegion = new ArrayList();

        for(int i = 0; i < nExons; ++i) {
            Tuple2<Integer, Integer> r = new Tuple2(exonStarts.get(i), exonEnds.get(i));
            cdsRegion.add(r);
        }

        return cdsRegion;
    }

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

            int mRNAPosEnd;
            int mRNAPosStart = 0;
            for (CoordinatesRange cds : t.getCodingCoordinates())
            {
                mRNAPosEnd = mRNAPosStart + cds.getEnd()-cds.getStart();
                t.getmRNACoordinates().add(new CoordinatesRange(mRNAPosStart+1, mRNAPosEnd));
                t.getProteinCoordinates().add(new CoordinatesRange((int)Math.ceil(mRNAPosStart/3.0f)+1, (int)Math.ceil(mRNAPosEnd/3.0f)));
                mRNAPosStart = mRNAPosEnd;
            }
            t.setHasAlternativeExons(annotation.getBoolean(annotation.fieldIndex(CommonConstants.COL_HAS_ALTERNATIVE_EXONS)));
            t.setAlternativeExons(annotation.getList(annotation.fieldIndex(CommonConstants.COL_ALTERNATIVE_EXONS)));

            t.setMoleculeId(annotation.getString(annotation.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
            t.setSequence(annotation.getString(annotation.fieldIndex(CommonConstants.COL_PROTEIN_SEQUENCE)));

            m.getTranscripts().add(t);
        }
        return m;
    }
}