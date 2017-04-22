package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.utils.MapUtils;
import org.rcsb.correlatedexons.utils.RowUtils;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by yana on 4/17/17.
 */
public class MapToBestStructure implements Function<Tuple2<String, Iterable<Row>>, List<Row>> {

    @Override
    public List<Row> call(Tuple2<String, Iterable<Row>> data) throws Exception {

        Map<String, List<String>> map = MapUtils.getMapFromIterator(data._2);

        if (map.size()==0)
            return null;

        Map.Entry<String, List<String>> bestpdb = map.entrySet().stream().filter(e -> e.getKey().contains("pdb")).max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1).get();
        int max_pdb_coverage = bestpdb.getValue().size();

        Map.Entry<String, List<String>> bestmodel = map.entrySet().stream().filter(e -> e.getKey().contains("model")).max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1).get();
        int max_model_coverage = bestmodel.getValue().size();

        List<String> bestmatches;
        // select based on resolution for the PDB structure preferably (if the coverage of the )
        if ( max_model_coverage - max_pdb_coverage > 2 ) {
            bestmatches = map.entrySet().stream().filter(e -> e.getValue().size() == max_model_coverage).map(e -> e.getKey()).collect(Collectors.toList());
        }
        else {
            bestmatches = map.entrySet().stream().filter(e -> e.getValue().size() == max_pdb_coverage).map(e -> e.getKey()).collect(Collectors.toList());
        }

        Iterator<Row> it = data._2.iterator();
        while (it.hasNext()) {

            Row row = it.next();

            String pdbId = RowUtils.getPdbId(row);
            String chainId = RowUtils.getChainId(row);
        }


//        String pdbId = maxkey.split("_")[0];
//        String chainId = maxkey.split("_")[1];
//
//        List<Row> best = CommonUtils.getBestPDBStructure(data._2, pdbId, chainId);
//        if (best.size() == 0) {
//            best = CommonUtils.getBestModelStructure(data._2, pdbId, chainId);
//        }
//        if (best.size() <= 1) {
//            return null;
//        }
        return null;
    }
}
