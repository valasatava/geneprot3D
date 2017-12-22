package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.tools.CoordinatesTool;
import org.rcsb.mojave.genomemapping.MultipleFeaturesMap;
import org.rcsb.mojave.mappers.PositionMapping;
import org.rcsb.mojave.mappers.SegmentMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 12/11/17.
 */
public class SliceExonsCoordinatesStructureCentric implements Function<Row, MultipleFeaturesMap> {

    @Override
    public MultipleFeaturesMap call(Row row) throws Exception {

        MultipleFeaturesMap featuresMap = new MultipleFeaturesMap();

        featuresMap.setChromosome(row.getString(row.fieldIndex(CommonConstants.COL_CHROMOSOME)));
        featuresMap.setOrientation(row.getString(row.fieldIndex(CommonConstants.COL_ORIENTATION)));
        featuresMap.setGeneId(row.getString(row.fieldIndex(CommonConstants.COL_GENE_ID)));
        featuresMap.setGeneName(row.getString(row.fieldIndex(CommonConstants.COL_GENE_NAME)));
        featuresMap.setTranscriptId(row.getString(row.fieldIndex(CommonConstants.COL_TRANSCRIPT_ID)));
        featuresMap.setTranscriptName(row.getString(row.fieldIndex(CommonConstants.COL_TRANSCRIPT_NAME)));
        featuresMap.setUniProtId(row.getString(row.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)));
        featuresMap.setMoleculeId(row.getString(row.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
        featuresMap.setCanonical(row.getBoolean(row.fieldIndex(CommonConstants.COL_CANONICAL)));
        featuresMap.setEntryId(row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID)));
        featuresMap.setEntityId(row.getString(row.fieldIndex(CommonConstants.COL_ENTITY_ID)));
        featuresMap.setChainId(row.getString(row.fieldIndex(CommonConstants.COL_CHAIN_ID)));

        List<Row> mappingGenomic = row.getList(row.fieldIndex(CommonConstants.COL_COORDINATES+"Genomic"))
                .stream().map(v -> (Row) v).collect(Collectors.toList());
        List<Row> mappingEntity = row.getList(row.fieldIndex(CommonConstants.COL_COORDINATES+"Entity"))
                .stream().map(v -> (Row) v).collect(Collectors.toList());

        List<SegmentMapping> coordinates = new ArrayList<>();

        for (Row r : mappingGenomic) {

            Row gs = r.getStruct(r.fieldIndex(CommonConstants.COL_START));
            Row ge = r.getStruct(r.fieldIndex(CommonConstants.COL_END));

            int unpGenStart = gs.getInt(gs.fieldIndex(CommonConstants.COL_UNIPROT_POSITION));
            int unpGenEnd = ge.getInt(ge.fieldIndex(CommonConstants.COL_UNIPROT_POSITION));

            List<Row> mappedToEntity = mappingEntity
                    .stream().filter(e -> (CoordinatesTool.hasOverlap(unpGenStart, unpGenEnd
                            , e.getStruct(e.fieldIndex(CommonConstants.COL_START)).getInt(e.getStruct(e.fieldIndex(CommonConstants.COL_START)).fieldIndex(CommonConstants.COL_UNIPROT_POSITION))
                            , e.getStruct(e.fieldIndex(CommonConstants.COL_END)).getInt(e.getStruct(e.fieldIndex(CommonConstants.COL_END)).fieldIndex(CommonConstants.COL_UNIPROT_POSITION)))
                    )).collect(Collectors.toList());

            if (mappedToEntity.size() != 0) {

                Row e = mappedToEntity.get(0);
                Row es = e.getStruct(e.fieldIndex(CommonConstants.COL_START));
                Row ee = e.getStruct(e.fieldIndex(CommonConstants.COL_END));

                int genStart = gs.getInt(gs.fieldIndex(CommonConstants.COL_GENOMIC_POSITION));
                int genEnd = ge.getInt(ge.fieldIndex(CommonConstants.COL_GENOMIC_POSITION));

                int mRnaStart = gs.getInt(gs.fieldIndex(CommonConstants.COL_MRNA_POSITION));
                int mRnaEnd = ge.getInt(ge.fieldIndex(CommonConstants.COL_MRNA_POSITION));

                int unpStrStart = es.getInt(es.fieldIndex(CommonConstants.COL_UNIPROT_POSITION));
                int unpStrEnd = ee.getInt(ee.fieldIndex(CommonConstants.COL_UNIPROT_POSITION));

                int pdbStart = es.getInt(es.fieldIndex(CommonConstants.COL_PDBSEQ_POSITION));
                int pdbEnd = ee.getInt(ee.fieldIndex(CommonConstants.COL_PDBSEQ_POSITION));

                int[] overlap = CoordinatesTool.getOverlap(unpGenStart, unpGenEnd, unpStrStart, unpStrEnd);
                int oStart = overlap[0];
                int oEnd = overlap[1];

                // Adjust genomic coordinates by overlap
                int genStartAdjusted = CoordinatesTool.adjustStartCoordinateBaseOneToThree(unpGenStart, oStart, genStart);
                int genEndAdjusted = CoordinatesTool.adjustEndCoordinateBaseOneToThree(unpGenEnd, oEnd, genEnd);

                // Adjust mRNA coordinates by overlap
                int mRnaStartAdjusted = CoordinatesTool.adjustStartCoordinateBaseOneToThree(unpGenStart, oStart, mRnaStart);
                int mRnaEndAdjusted = CoordinatesTool.adjustEndCoordinateBaseOneToThree(unpGenEnd, oEnd, mRnaEnd);

                // Adjust structure coordinates by overlap
                int pdbStartAdjusted = CoordinatesTool.adjustStartCoordinateBaseOne(unpStrStart, oStart, pdbStart);
                int pdbEndAdjusted = CoordinatesTool.adjustEndCoordinateBaseOne(unpStrEnd, oEnd, pdbEnd);

                PositionMapping start = new PositionMapping();
                start.setGenomicPosition(genStartAdjusted);
                start.setmRNAPosition(mRnaStartAdjusted);
                start.setUniProtPosition(oStart);
                start.setPdbSeqPosition(pdbStartAdjusted);

                PositionMapping end = new PositionMapping();
                end.setGenomicPosition(genEndAdjusted);
                end.setmRNAPosition(mRnaEndAdjusted);
                end.setUniProtPosition(oEnd);
                end.setPdbSeqPosition(pdbEndAdjusted);

                SegmentMapping segment = new SegmentMapping();
                segment.setId(r.getInt(r.fieldIndex("id")));
                segment.setStart(start);
                segment.setEnd(end);
                coordinates.add(segment);
            }
        }

        featuresMap.setCoordinates(coordinates);

        return featuresMap;
    }
}
