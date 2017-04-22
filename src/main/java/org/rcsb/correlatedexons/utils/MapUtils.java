package org.rcsb.correlatedexons.utils;

import org.apache.spark.sql.Row;

import java.util.*;

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
}