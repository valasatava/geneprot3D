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
public class MapToUniprotFeature implements Function<Row, Row> {

    private static String featureType;
    public MapToUniprotFeature(String feature) {
        featureType = feature;
    }

    @Override
    public Row call(Row row) throws Exception {

        String uniProtId = RowUtils.getUniProtId(row);
        uniProtId = uniProtId.split(",")[0];

        int uniStart = RowUtils.getUniProtStart(row);
        int uniEnd = RowUtils.getUniProtEnd(row);

        if (uniStart == -1 || uniEnd == -1)
            return null;

        Range<Integer> exonRange = Range.closed(uniStart, uniEnd);

        Row mapped = null;
        Uniprot up = RCSBUniProtMirror.getUniProtFromFile(uniProtId);
        try {
            for(Entry e : up.getEntry()) {
                for (FeatureType ft : e.getFeature()) {
                    if ( ft.getLocation() != null && ft.getLocation().getPosition() != null && ft.getLocation().getPosition().getPosition() != null) {

                        if ( ft.getType().equals(featureType) ) {

                            // ranged features
                            if ( ft.getLocation() != null && ft.getLocation().getBegin() != null && ft.getLocation().getEnd() != null)
                            {
                                Range<Integer> featureRange = Range.closed(ft.getLocation().getBegin().getPosition().intValue(),
                                        ft.getLocation().getEnd().getPosition().intValue());

                                if (featureRange.isConnected(exonRange)) {
                                    Range<Integer> coveredRange = featureRange.intersection(exonRange);
                                    mapped = RowUtils.mapToFeatureRange(ft, coveredRange, row);
                                }
                            }
                            // single resiude position features
                            else if ( ft.getLocation() != null && ft.getLocation().getPosition() != null && ft.getLocation().getPosition().getPosition() != null)
                            {
                                if ( exonRange.contains(ft.getLocation().getPosition().getPosition().intValue()) ) {
                                    mapped = RowUtils.mapToFeatureResidue(ft, ft.getLocation().getPosition().getPosition().intValue(), row);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("NOT FOUND: "+uniProtId);
        }
        return mapped;
    }
}
