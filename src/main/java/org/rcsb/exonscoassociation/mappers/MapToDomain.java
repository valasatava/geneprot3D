package org.rcsb.exonscoassociation.mappers;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.uniprot.auto.Entry;
import org.rcsb.uniprot.auto.FeatureType;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.config.RCSBUniProtMirror;

/**
 * Created by yana on 4/25/17.
 */
public class MapToDomain implements Function<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {

        String uniProtId = RowUtils.getUniProtId(row);

        int uniStart = RowUtils.getUniProtStart(row);
        int uniEnd = RowUtils.getUniProtEnd(row);

        if (uniStart == -1 || uniEnd == -1)
            return null;

        Range<Integer> exonRange = Range.closed(uniStart, uniEnd);

        boolean flag = false;
        Uniprot up = RCSBUniProtMirror.getUniProtFromFile(uniProtId);
        for(Entry e : up.getEntry()) {
            
            for (FeatureType ft : e.getFeature()) {

                if (ft.getLocation() != null && ft.getLocation().getBegin() != null && ft.getLocation().getEnd() != null) {

                    if ( ft.getType().equals("domain") ) {

                        Range<Integer> featureRange = Range.closed(ft.getLocation().getBegin().getPosition().intValue(),
                                ft.getLocation().getEnd().getPosition().intValue());
                        if (featureRange.isConnected(exonRange)) {
                            Range<Integer> coveredRange = featureRange.intersection(exonRange);
                            row = RowUtils.addField(row, ft.getDescription());
                            row = RowUtils.addField(row, coveredRange.lowerEndpoint());
                            row = RowUtils.addField(row, coveredRange.upperEndpoint());
                            flag = true;
                        }
                    }
                }
            }
        }

        if ( flag ) {
            return row;
        }
        return null;
    }
}
