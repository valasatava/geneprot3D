package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.utils.CommonUtils;
import org.rcsb.correlatedexons.utils.MapUtils;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by yana on 4/17/17.
 */
public class MapToBestStructure implements Function<Tuple2<String, Iterable<Row>>, List<Row>> {

    @Override
    public List<Row> call(Tuple2<String, Iterable<Row>> data) throws Exception {

        Iterator<Row> it = data._2.iterator();

        Map<String, List<String>> map = MapUtils.getMapFromIterator(it);

        if (map.size()==0)
            return null;

        String maxkey = map.entrySet().stream().max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1).get().getKey();

        String pdbId = maxkey.split("_")[0];
        String chainId = maxkey.split("_")[1];

        List<Row> best = CommonUtils.getBestPDBStructure(data._2, pdbId, chainId);
        if (best.size() == 0) {
            best = CommonUtils.getBestModelStructure(data._2, pdbId, chainId);
        }
        if (best.size() <= 1) {
            return null;
        }
        return best;
    }
}
