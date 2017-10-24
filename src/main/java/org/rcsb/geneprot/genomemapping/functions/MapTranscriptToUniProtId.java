package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.rcsb.mojave.util.CommonConstants;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Created by Yana Valasatava on 10/24/17.
 */
public class MapTranscriptToUniProtId implements FlatMapFunction<Row, Tuple3<Row, String, String>>, Serializable {

    private Map<String, Iterable<String>> map;
    public MapTranscriptToUniProtId(Broadcast<Map<String, Iterable<String>>> bc) {
        map = bc.value();
    }
    
    @Override
    public Iterator<Tuple3<Row, String, String>> call(Row row) throws Exception {

        List<Tuple3<Row, String, String>> list = new ArrayList<>();

        String geneName = row.getString(row.schema().fieldIndex(CommonConstants.COL_GENE_NAME));
        if (map.keySet().contains(geneName))
        {
            Iterator<String> it = map.get(geneName).iterator();
            while (it.hasNext()) {
                String uniProtId = it.next();
                list.add(new Tuple3<>(row, org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION, uniProtId));
            }
        }
        return list.iterator();
    }
}