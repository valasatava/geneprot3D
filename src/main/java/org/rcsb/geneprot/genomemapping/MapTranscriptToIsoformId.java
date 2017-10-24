package org.rcsb.geneprot.genomemapping;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import scala.Tuple3;


/**
 * Created by Yana Valasatava on 10/24/17.
 */
public class MapTranscriptToIsoformId implements Function<Row, Tuple3<Row, String, String>> {

    @Override
    public Tuple3<Row, String, String> call(Row row) throws Exception {

        String moleculeId = row.getString(row.schema().fieldIndex(CommonConstants.MOLECULE_ID));
        String id = moleculeId.split("-")[1];
        return new Tuple3<>(row, CommonConstants.ISOFORM_ID, id);
    }
}
