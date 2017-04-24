package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.utils.CommonUtils;
import org.rcsb.correlatedexons.utils.MapUtils;
import org.rcsb.correlatedexons.utils.RowUtils;
import scala.Tuple2;

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

        int max_pdb_coverage = 0;
        int max_model_coverage = 0;

        try {
            Map.Entry<String, List<String>> bestpdb = map.entrySet().stream()
                    .filter(e -> e.getKey().contains("pdb"))
                    .max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1).get();
            max_pdb_coverage = bestpdb.getValue().size();
        } catch (Exception e) {
        }

        try {
            Map.Entry<String, List<String>> bestmodel = map.entrySet().stream()
                    .filter(e -> e.getKey().contains("model"))
                    .max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1).get();

            max_model_coverage = bestmodel.getValue().size();
        } catch (Exception e) {
        }

        List<String> bestCoverage;
        // select based on resolution for the PDB structure preferably (if the coverage of the )
        if ( max_model_coverage - max_pdb_coverage > 2 || max_pdb_coverage == 0 ) {
            int finalMax_model_coverage = max_model_coverage;
            bestCoverage = map.entrySet().stream()
                    .filter(e -> (e.getValue().size() == finalMax_model_coverage && e.getKey()
                            .contains("model"))).map(e -> e.getKey()).collect(Collectors.toList());
        }
        else {
            int finalMax_pdb_coverage = max_pdb_coverage;
            bestCoverage = map.entrySet().stream()
                    .filter(e -> (e.getValue().size() == finalMax_pdb_coverage && e.getKey()
                            .contains("pdb"))).map(e -> e.getKey()).collect(Collectors.toList());
        }

        String pdbIdbest = "";
        String chainIdbest = "";
        if (bestCoverage.size()>1) {
            float bestRes = 99.9f;

            for (String key : bestCoverage) {

                String pdbIdb = key.split("_")[0];
                String chainId = key.split("_")[1];

                List<Row> lst = CommonUtils.getPDBStructure(data._2, pdbIdb, chainId);
                if (lst.size()==0) {
                    lst = CommonUtils.getModelStructure(data._2, pdbIdb, chainId);
                }
                Row r = lst.get(0);
                if ( RowUtils.getResolution(r) < bestRes ) {
                    pdbIdbest = RowUtils.getPdbId(r);
                    chainIdbest = RowUtils.getChainId(r);
                }
            }
        }
        else {
            String bestmatch = null;
            try {
                bestmatch = bestCoverage.get(0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            pdbIdbest = bestmatch.split("_")[0];
            chainIdbest = bestmatch.split("_")[1];
        }
        List<Row> best = CommonUtils.getPDBStructure(data._2, pdbIdbest, chainIdbest);
        return best;
    }
}
