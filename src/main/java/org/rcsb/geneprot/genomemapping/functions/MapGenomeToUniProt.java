package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.geneprot.genomemapping.model.Transcript;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class MapGenomeToUniProt implements Function<Row, GenomeToUniProtMapping> {

    private static List<Tuple2<Integer, Integer>> getCDSRegions(List<Integer> origExonStarts, List<Integer> origExonEnds, int cdsStart, int cdsEnd)
    {
        List<Integer> exonStarts = new ArrayList(origExonStarts);
        List<Integer> exonEnds = new ArrayList(origExonEnds);
        int j = 0;

        int nExons;
        for(nExons = 0; nExons < origExonStarts.size(); ++nExons) {
            if(((Integer)origExonEnds.get(nExons)).intValue() >= cdsStart && ((Integer)origExonStarts.get(nExons)).intValue() <= cdsEnd) {
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

    @Override
    public GenomeToUniProtMapping call(Row row) throws Exception {

        GenomeToUniProtMapping m = new GenomeToUniProtMapping();

        m.setChromosome(row.getString(row.fieldIndex(CommonConstants.CHROMOSOME)));
        m.setGeneName(row.getString(row.fieldIndex(CommonConstants.GENE_NAME)));
        m.setOrientation(row.getString(row.fieldIndex(CommonConstants.ORIENTATION)));
        m.setUniProtId(row.getString(row.fieldIndex(CommonConstants.UNIPROT_ID)));

        List<Row> annotations = row.getList(row.fieldIndex(CommonConstants.TRANSCRIPTS));

        for (Row annotation : annotations)
        {
            Transcript t = new Transcript();

            t.setRnaSequenceIdentifier(annotation.getString(annotation.fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)));
            t.setProteinSequenceIdentifier(annotation.getString(annotation.fieldIndex(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION)));
            t.setMoleculeId(annotation.getString(annotation.fieldIndex(CommonConstants.MOLECULE_ID)));
            t.setIsoformId(annotation.getString(annotation.fieldIndex(CommonConstants.ISOFORM_ID)));

            t.setTranscriptionStart(annotation.getInt(annotation.fieldIndex(CommonConstants.TX_START)));
            t.setTranscriptionEnd(annotation.getInt(annotation.fieldIndex(CommonConstants.TX_END)));

            int cdsStart = annotation.getInt(annotation.fieldIndex(CommonConstants.CDS_START));
            int cdsEnd = annotation.getInt(annotation.fieldIndex(CommonConstants.CDS_END));
            t.setCdsStart(cdsStart);
            t.setCdsEnd(cdsEnd);

            t.setExonsCount(annotation.getInt(annotation.fieldIndex(CommonConstants.EXONS_COUNT)));

            List<Integer> exonsStart = annotation.getList(annotation.fieldIndex(CommonConstants.EXONS_START));
            List<Integer> exonsEnd = annotation.getList(annotation.fieldIndex(CommonConstants.EXONS_END));

            if ( m.getOrientation().equals("-") ) // 3 last bases encode for a stop codon
                cdsStart += 3;
            else
                cdsEnd -= 3;

            List<Tuple2<Integer, Integer>> cdsRegions = getCDSRegions(exonsStart, exonsEnd, cdsStart, cdsEnd);
            if (m.getOrientation().equals("-"))
                Collections.reverse(cdsRegions);

            int mRNAPosEnd;
            int mRNAPosStart = 0;
            for (Tuple2<Integer, Integer> cds : cdsRegions)
            {
                mRNAPosEnd = mRNAPosStart + cds._2-cds._1;

                t.getExonsCoordinates().add(new CoordinatesRange(cds._1, cds._2));
                t.getmRNACoordinates().add(new CoordinatesRange(mRNAPosStart+1, mRNAPosEnd));
                t.getProteinCoordinates().add(new CoordinatesRange((int)Math.ceil(mRNAPosStart/3.0f)+1, (int)Math.ceil(mRNAPosEnd/3.0f)));

                mRNAPosStart = mRNAPosEnd;
            }
            m.getTranscripts().add(t);
        }
        return m;
    }
}