package org.rcsb.geneprot.genomemapping.functions;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.utils.RowUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Yana Valasatava on 11/1/17.
 */
public class AnnotateAlternativeEvents implements FlatMapFunction<Iterable<Row>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(AnnotateAlternativeEvents.class);

    private static List<Range<Integer>> range(Row transcript) {

        List<Row> cds = transcript.getList(transcript.fieldIndex(CommonConstants.COL_CODING));

        List<Range<Integer>> regions = new ArrayList();
        for(Row r : cds) {
            regions.add(Range.closed(r.getInt(r.fieldIndex(CommonConstants.COL_START))
                                   , r.getInt(r.fieldIndex(CommonConstants.COL_END))));
        }
        return regions;
    }

    private static List<Range<Integer>> getRanges(List<Row> transcripts) {

        List list = new ArrayList();
        Iterator<Row> it = transcripts.iterator();
        while (it.hasNext()) {
            Row transcript = it.next();
            list.addAll(range(transcript));
        }
        return list;
    }

    private static Map<Integer, Integer> getHashKeys(List<Range<Integer>> ranges) {

        Map<Integer, Integer> map = new HashMap<>();
        for (Range<Integer> range : ranges) {
            if (map.containsKey(range.hashCode())) {
                int i = map.get(range.hashCode());
                map.put(range.hashCode(), i+1);
            }
            else 
                map.put(range.hashCode(), 1);
        }
        return map;
    }

    @Override
    public Iterator<Row> call(Iterable<Row> it) throws Exception {

        List<Row> list = new ArrayList<>();
        it.iterator().forEachRemaining(e -> list.add(e));
        int n = list.size();
        
        List<Row> updated = new ArrayList<>();
        logger.info("Calculate alternative events for {}", list.get(0).getString(list.get(0).fieldIndex(CommonConstants.COL_GENE_NAME)));
        try {
            if (list.size() == 1) {
                Row row = list.get(0);
                row = RowUpdater.addArray(row, CommonConstants.COL_ALTERNATIVE_EXONS, new ArrayList<>(), DataTypes.BooleanType);
                updated.add(RowUpdater.addField(row, CommonConstants.COL_HAS_ALTERNATIVE_EXONS, false, DataTypes.BooleanType));

            } else {
                Map<Integer, Integer> hashKeys = getHashKeys(getRanges(list));
                for (Row transcript : list) {
                    boolean hasAlternativeExons = false;
                    List<Boolean> map = new ArrayList<>();
                    Set<Integer> txKeys = getHashKeys(range(transcript)).keySet();
                    for (Integer key : txKeys) {
                        boolean flag = false;
                        if (hashKeys.get(key) != n) {
                            if (!hasAlternativeExons)
                                hasAlternativeExons = true;
                            flag = true;
                        }
                        map.add(flag);
                    }
                    Row row = RowUpdater.addArray(transcript, CommonConstants.COL_ALTERNATIVE_EXONS, map, DataTypes.BooleanType);
                    updated.add(RowUpdater.addField(row, CommonConstants.COL_HAS_ALTERNATIVE_EXONS, hasAlternativeExons, DataTypes.BooleanType));
                }
            }
        } catch (Exception e) {
            logger.error("Error has occurred while calculating alternative events {} : {}", e.getCause(), e.getMessage());
        }
        return updated.iterator();
    }
}
