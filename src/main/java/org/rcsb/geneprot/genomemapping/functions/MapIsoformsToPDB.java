package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.IsoformToPDB;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class MapIsoformsToPDB implements Function<Row, IsoformToPDB> {

    @Override
    public IsoformToPDB call(Row row) throws Exception {

        IsoformToPDB m = new IsoformToPDB();

        m.setMoleculeId(row.getString(row.fieldIndex(CommonConstants.COL_MOLECULE_ID)));

        return null;
    }
}
