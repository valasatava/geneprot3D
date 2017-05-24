package org.rcsb.exonscoassociation.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.PDBHeader;
import org.biojava.nbio.structure.Structure;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.ReaderUtils;

/**
 * Created by yana on 4/19/17.
 */
public class MapToResolution implements Function<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {

        String pdbId = RowUtils.getPdbId(row);

        // TODO: write a better handling
        if ( pdbId.equals("4NL7") || pdbId.equals("4NL6") || pdbId.equals("3G07")) {
            return null;
        }

        float resolution = StructureUtils.getResolution(pdbId);

        return RowUtils.addField(row, resolution);
    }
}
