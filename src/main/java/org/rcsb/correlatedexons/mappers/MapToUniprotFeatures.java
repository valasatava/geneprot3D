package org.rcsb.correlatedexons.mappers;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.utils.RowUtils;
import org.rcsb.uniprot.auto.Entry;
import org.rcsb.uniprot.auto.FeatureType;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.config.RCSBUniProtMirror;

/**
 * Created by yana on 4/25/17.
 */
public class MapToUniprotFeatures implements Function<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {

        String uniProtId = RowUtils.getUniProtId(row);

        int uniStart = ( RowUtils.getUniProtStart(row) != -1 ) ? RowUtils.getUniProtStart(row) : Integer.MAX_VALUE;
        int uniEnd = ( RowUtils.getUniProtEnd(row) != -1 ) ? RowUtils.getUniProtEnd(row) : Integer.MIN_VALUE;

        Range<Integer> exonRange = Range.closed(uniStart, uniEnd);

        Uniprot up = RCSBUniProtMirror.getUniProtFromFile(uniProtId);
        for(Entry e : up.getEntry()) {
            
            for (FeatureType ft : e.getFeature()) {

                // ranged features
                if (ft.getLocation() != null && ft.getLocation().getBegin() != null && ft.getLocation().getEnd() != null) {

                    if ( ft.getType().equals("topological domain") ) {

                        Range<Integer> featureRange = Range.closed(ft.getLocation().getBegin().getPosition().intValue(), ft.getLocation().getEnd().getPosition().intValue());

                        if (featureRange.isConnected(exonRange)) {

                            Range<Integer> intersrction = featureRange.intersection(exonRange);

                            row = RowUtils.addField(row, ft.getDescription());
                            row = RowUtils.addField(row, intersrction.lowerEndpoint());
                            row = RowUtils.addField(row, intersrction.upperEndpoint());

                        }
                    }

                    else if (ft.getType().equals("topological domain")) {

                    }

                }
            }
        }

        return null;
    }
}
