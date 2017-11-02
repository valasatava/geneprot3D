package org.rcsb.geneprot.genomemapping;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.utils.RowUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Yana Valasatava on 10/24/17.
 */
public class MapTranscriptToIsoformId implements Function<Row, Row> {

    private static final Logger logger = LoggerFactory.getLogger(MapTranscriptToIsoformId.class);

    @Override
    public Row call(Row row) throws Exception {

        String moleculeId = row.getString(row.schema().fieldIndex(CommonConstants.COL_MOLECULE_ID));
        String id = moleculeId.split("-")[1];
        logger.debug("Updated field '{}' for {} ", CommonConstants.COL_ISOFORM_ID, moleculeId);

        return RowUpdater.updateField(row, CommonConstants.COL_ISOFORM_ID, id);
    }
}