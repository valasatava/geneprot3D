package org.rcsb.correlatedexons.utils;

import org.apache.spark.sql.Row;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yana on 4/20/17.
 */
public class MapUtils {

    public static Map<String, List<String>> getMapFromIterator(Iterable<Row> data ) {

        Map<String, List<String>> map = new HashMap<String, List<String>>();

        Iterator<Row> it = data.iterator();
        while (it.hasNext()) {

            Row row = it.next();

            String pdbId = RowUtils.getPdbId(row);
            String chainId = RowUtils.getChainId(row);

            // TODO: write a better handling
            if ( pdbId.equals("4NL7") || pdbId.equals("4NL6"))
                continue;

            String key = pdbId+"_"+chainId;
            if ( RowUtils.isPDBStructure(row) ) {
                key += "_pdb";
            }
            else {
                key += "_model";
            }
            String exon = RowUtils.getExon(row);

            if ( !map.keySet().contains(key) ) {
                List<String> lst = new ArrayList<String>() {{ add(exon); }};
                map.put(key, lst);
            }
            else if ( !map.get(key).contains(exon) ) {
                List<String> lst = map.get(key);
                lst.add(exon);
                map.put(key,lst);
            }
        }
        return map;
    }

    public static int getBestCoverageValue(Map<String, List<String>> map, String key) {

        int max_coverage = 0;
        Stream<Map.Entry<String, List<String>>> pdbs = map.entrySet().stream()
                .filter(e -> e.getKey().contains(key));
        if ( pdbs.count() != 0 ) {
            max_coverage = map.entrySet().stream()
                    .filter(e -> e.getKey().contains(key))
                    .max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1)
                    .get().getValue().size();
        }
        return max_coverage;
    }

    public static List<String> getKeysWithBestCoverage(Map<String, List<String>> map, String key, int coverage) {

        List<String> keys = map.entrySet().stream()
                .filter(e -> (e.getValue().size() == coverage && e.getKey()
                        .contains(key))).map(e -> e.getKey()).collect(Collectors.toList());
        return keys;
    }
}