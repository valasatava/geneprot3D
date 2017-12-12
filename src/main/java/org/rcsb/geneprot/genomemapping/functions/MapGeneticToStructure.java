package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.mojave.genomemapping.GenomicToStructureMapping;
import org.rcsb.mojave.mappers.PositionMapping;
import org.rcsb.mojave.mappers.SegmentMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 12/11/17.
 */
public class MapGeneticToStructure implements Function<Row, GenomicToStructureMapping> {

    @Override
    public GenomicToStructureMapping call(Row row) throws Exception {

        GenomicToStructureMapping gsm = new GenomicToStructureMapping();

        gsm.setChromosome(row.getString(row.fieldIndex(CommonConstants.COL_CHROMOSOME)));
        gsm.setOrientation(row.getString(row.fieldIndex(CommonConstants.COL_ORIENTATION)));
        gsm.setGeneId(row.getString(row.fieldIndex(CommonConstants.COL_GENE_ID)));
        gsm.setGeneName(row.getString(row.fieldIndex(CommonConstants.COL_GENE_NAME)));
        gsm.setTranscriptId(row.getString(row.fieldIndex(CommonConstants.COL_TRANSCRIPT_ID)));
        gsm.setTranscriptName(row.getString(row.fieldIndex(CommonConstants.COL_TRANSCRIPT_NAME)));
        gsm.setUniProtId(row.getString(row.fieldIndex(CommonConstants.COL_UNIPROT_ACCESSION)));
        gsm.setMoleculeId(row.getString(row.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
        gsm.setCanonical(row.getBoolean(row.fieldIndex(CommonConstants.COL_CANONICAL)));
        gsm.setEntryId(row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID)));
        gsm.setEntityId(row.getString(row.fieldIndex(CommonConstants.COL_ENTITY_ID)));
        gsm.setChainId(row.getString(row.fieldIndex(CommonConstants.COL_CHAIN_ID)));

        List<Row> mappingGenomic = row.getList(row.fieldIndex("coordinatesMappingGenomic"))
                .stream().map(v -> (Row) v).collect(Collectors.toList());
        List<Row> mappingEntity = row.getList(row.fieldIndex("coordinatesMappingEntity"))
                .stream().map(v -> (Row) v).collect(Collectors.toList());

        List<SegmentMapping> mapping = new ArrayList<>();

        for (Row r : mappingGenomic) {

            Row startGen = r.getStruct(r.fieldIndex(CommonConstants.COL_START));
            Row endGen = r.getStruct(r.fieldIndex(CommonConstants.COL_END));

            int uniStart = startGen.getInt(startGen.fieldIndex("uniProtPosition"));
            int uniEnd = endGen.getInt(endGen.fieldIndex("uniProtPosition"));

            List<Row> mappedToEntity = mappingEntity
                    .stream()
                    .filter(e -> (
                            e.getStruct(e.fieldIndex(CommonConstants.COL_START)).getInt(e.getStruct(e.fieldIndex(CommonConstants.COL_START)).fieldIndex("uniProtPosition")) <= uniStart
                            &&
                            e.getStruct(e.fieldIndex(CommonConstants.COL_END)).getInt(e.getStruct(e.fieldIndex(CommonConstants.COL_END)).fieldIndex("uniProtPosition")) >= uniEnd
                    )).collect(Collectors.toList());

            if (mappedToEntity.size() != 0) {

                Row struct = mappedToEntity.get(0);
                Row startStruct = struct.getStruct(struct.fieldIndex(CommonConstants.COL_START));
                int uniStructStart = startStruct.getInt(startStruct.fieldIndex("uniProtPosition"));
                int pdbStructStart = startStruct.getInt(startStruct.fieldIndex("seqResPosition"));
                int d1 = uniStart - uniStructStart;
                int pdbStart = pdbStructStart + d1;

                Row endStruct = struct.getStruct(struct.fieldIndex(CommonConstants.COL_END));
                int pdbStructEnd = endStruct.getInt(endStruct.fieldIndex("seqResPosition"));
                int d2 = uniEnd - uniStart;
                int pdbEnd = pdbStart + d2;

                if (pdbEnd > pdbStructEnd)
                    pdbEnd = pdbStructEnd;

                if (pdbEnd == 0 || pdbEnd <= pdbStart)
                    continue;

                PositionMapping pStart = new PositionMapping();
                pStart.setGeneticPosition(startGen.getInt(startGen.fieldIndex("geneticPosition")));
                pStart.setmRNAPosition(startGen.getInt(startGen.fieldIndex("mRNAPosition")));
                pStart.setUniProtPosition(startGen.getInt(startGen.fieldIndex("uniProtPosition")));
                pStart.setSeqResPosition(pdbStart);

                PositionMapping pEnd = new PositionMapping();
                pEnd.setGeneticPosition(endGen.getInt(endGen.fieldIndex("geneticPosition")));
                pEnd.setmRNAPosition(endGen.getInt(endGen.fieldIndex("mRNAPosition")));
                pEnd.setUniProtPosition(endGen.getInt(endGen.fieldIndex("uniProtPosition")));
                pEnd.setSeqResPosition(pdbEnd);

                SegmentMapping segment = new SegmentMapping();
                segment.setId(r.getInt(r.fieldIndex("id")));
                segment.setStart(pStart);
                segment.setEnd(pEnd);
                mapping.add(segment);

            }
        }

        gsm.setCoordinatesMapping(mapping);

        return gsm;
    }
}
