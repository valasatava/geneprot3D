package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.PDBHeader;
import org.biojava.nbio.structure.Structure;
import org.rcsb.correlatedexons.utils.RowUtils;
import org.rcsb.correlatedexons.utils.StructureUtils;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.ReaderUtils;

/**
 * Created by yana on 4/19/17.
 */
public class MapToResolution implements Function<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {

        Float resolution;
        String pdbId = RowUtils.getPdbId(row);

        // TODO: write a better handling
        if ( pdbId.equals("4NL7") || pdbId.equals("4NL6")) {
            return null;
        }

        try {
            MmtfStructure mmtfData = ReaderUtils.getDataFromUrl(pdbId);
            resolution = mmtfData.getResolution();

        } catch (Exception e) {
            Structure structure = StructureUtils.getBioJavaStructure(pdbId);
            PDBHeader header = structure.getPDBHeader();
            resolution = header.getResolution();
        }

        return RowUtils.addField(row, resolution);
    }
}
