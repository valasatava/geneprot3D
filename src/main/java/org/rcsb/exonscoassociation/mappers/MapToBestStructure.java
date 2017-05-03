package org.rcsb.exonscoassociation.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.rcsb.exonscoassociation.utils.CommonUtils;
import org.rcsb.exonscoassociation.utils.MapUtils;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Created by yana on 4/17/17.
 */
public class MapToBestStructure implements Function<Tuple2<String, Iterable<Row>>, List<Row>> {

    @Override
    public List<Row> call(Tuple2<String, Iterable<Row>> data) throws Exception {

        Map<String, List<String>> map = MapUtils.getMapFromIterator(data._2);

        if (map.size()==0)
            return null;

        // select best coverage
        int max_pdb_coverage = MapUtils.getBestCoverageValue(map, "pdb");
        int max_model_coverage = MapUtils.getBestCoverageValue(map, "model");

        // select based on resolution for the PDB structure
        String key="pdb";
        int max_coverage = max_pdb_coverage;
        if ( max_model_coverage - max_pdb_coverage >= 1 || max_pdb_coverage == 0 ) {
            key="model";
            max_coverage = max_model_coverage;
        }

        List<String> structures = MapUtils.getKeysWithBestCoverage(map, key, max_coverage);
        //TODO add the range handling
        String[] beststructure = CommonUtils.getStructureWithBestResolution(data._2, structures);

        if (beststructure[0]==null)
            return null;

        List<Row> best;
        if (key.equals("pdb")) {
            best = CommonUtils.getPDBStructure(data._2, beststructure);
        }
        else {
            best = CommonUtils.getModelStructure(data._2, beststructure);
        }
        return best;
    }
}
