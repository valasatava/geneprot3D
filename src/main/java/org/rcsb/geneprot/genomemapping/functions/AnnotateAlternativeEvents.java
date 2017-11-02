package org.rcsb.geneprot.genomemapping.functions;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.genomemapping.utils.RowUpdater;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 11/1/17.
 */
public class AnnotateAlternativeEvents implements Function<Iterable<Row>, Iterable<Row>> {

    private static List<Range<Integer>> range(Row transcript) {

        List<Integer> start = transcript.getList(transcript.fieldIndex(CommonConstants.EXONS_START));
        List<Integer> end = transcript.getList(transcript.fieldIndex(CommonConstants.EXONS_END));

        int cdsStart = transcript.getInt(transcript.fieldIndex(CommonConstants.CDS_START));
        int cdsEnd = transcript.getInt(transcript.fieldIndex(CommonConstants.CDS_END));

        List<Integer> exonStarts = new ArrayList(start);
        List<Integer> exonEnds = new ArrayList(end);
        int j = 0;


        for(int nExons = 0; nExons < start.size(); ++nExons) {
            if((end.get(nExons)).intValue() >= cdsStart && (start.get(nExons)).intValue() <= cdsEnd) {
                ++j;
            } else {
                exonStarts.remove(j);
                exonEnds.remove(j);
            }
        }

        List<Range<Integer>> cdsRegions = new ArrayList();
        for(int i = 0; i < exonStarts.size(); ++i) {
            Range<Integer> r = Range.closed(exonStarts.get(i), exonEnds.get(i));
            cdsRegions.add(r);
        }

        return cdsRegions;
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

    private static Set<Integer> getHashKeys(List<Range<Integer>> ranges) {

        Set<Integer> set = new HashSet<>();
        for (Range<Integer> range : ranges) {
            Tuple2<Integer, Integer> t = new Tuple2<>(range.lowerEndpoint(), range.upperEndpoint());
            set.add(t.hashCode());
        }
        return set;
    }

    private static Set<Integer> getHashKeysAsSet(List<Row> list) {
        List<Range<Integer>> ranges = getRanges(list);
        return getHashKeys(ranges);
    }

    @Override
    public Iterable<Row> call(Iterable<Row> it) throws Exception {

        List<Row> list = new ArrayList<>();
        it.iterator().forEachRemaining(e -> list.add(e));

        List<Row> updated = new ArrayList<>();

        if (list.size() == 1) {
            Row row = list.get(0);
            updated.add(RowUpdater.addField(row, CommonConstants.COL_HAS_ALTERNATIVE_EXONS, false, DataTypes.BooleanType));

        } else {
            Map<Integer, Boolean> map = new HashMap<>();
            Set<Integer> hashKeys = getHashKeysAsSet(list);
            boolean hasAlternativeExons = false;
            for ( Integer key : hashKeys ) {
                for (Row transcript : list) {
                    Set<Integer> txKeys = getHashKeys(range(transcript));
                    if (!txKeys.contains(key)) {
                        map.put(key, true);
                        if (!hasAlternativeExons)
                            hasAlternativeExons = true;
                        break;
                    }
                }
                if (!map.keySet().contains(key))
                    map.put(key, false);
            }

            for (Row row : list) {
                if (hasAlternativeExons) {
                    Set<Integer> keys = getHashKeys(range(row));
                    List<Boolean> flags = map.entrySet().stream().filter(e -> keys.contains(e.getKey())).map(e -> e.getValue()).collect(Collectors.toList());
                    row = RowUpdater.addArray(row, CommonConstants.COL_ALTERNATIVE_EXONS, flags, DataTypes.BooleanType);
                }
                updated.add(RowUpdater.addField(row, CommonConstants.COL_HAS_ALTERNATIVE_EXONS, hasAlternativeExons, DataTypes.BooleanType));
            }
        }
        return updated;
    }
}
