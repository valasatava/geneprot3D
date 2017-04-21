package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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

        String pdbId = RowUtils.getPdbId(row);

        Float resolution;
        try {
            MmtfStructure mmtfData = ReaderUtils.getDataFromUrl(pdbId);
            resolution = mmtfData.getResolution();

        } catch (Exception e) {
            Structure structure = StructureUtils.getBioJavaStructure(pdbId);
            PDBHeader header = structure.getPDBHeader();
            resolution = header.getResolution();
        }

        Object[] fields = new Object[row.length()+1];
        for (int c=0; c < row.length(); c++)
            fields[c] = row.get(c);
        fields[row.length()] = resolution;
        Row newRow = RowFactory.create(fields);

        return newRow;
    }
}
