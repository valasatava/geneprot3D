package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.util.List;

/**
 * Created by yana on 4/17/17.
 */
public class MapToBestStructure implements Function<String, Row> {

    private static final long serialVersionUID = -1796199023220653897L;

//    private Broadcast<JavaRDD<Row>> mbc = null;
//    public MapToBestStructure(Broadcast<JavaRDD<Row>> mapbc) {
//        mbc = mapbc;
//    }

    private String path = "";
    public MapToBestStructure(String chromosome) {
        path = chromosome;
    }

    @Override
    public Row call(String key) throws Exception {

        //JavaRDD<Row> map = mbc.value();

        String start = key.split("_")[0];
        String end = key.split("_")[1];
        String offset = key.split("_")[2];

        Dataset<Row> map = SaprkUtils.getSparkSession().read().parquet(path);
        JavaRDD<Row> exon = map.toJavaRDD().filter(t -> (  t.getInt(4)==Integer.valueOf(start)
                                            && t.getInt(5)==Integer.valueOf(end)
                                            && t.getInt(7)==Integer.valueOf(offset) ));

        List<Row> rows = exon.collect();

        Float bestres = 99.9f;
        Row bestrow = null;

        for (Row row : rows) {

            if ((!row.getString(11).equals("null")) && (!row.getString(15).equals("null")))
                System.out.println("ambiguity " + row.toString());

            Float resolution = row.getFloat(16);

            if (resolution == null)
                continue;

            if (resolution < bestres) {
                bestres = resolution;
                bestrow = row;
            }
        }

        if (bestres == 99.9f) {
            System.out.println("not assigned "+ key);
            return null;
        }

        System.out.println("assigned "+ bestrow.toString());
        return bestrow;

    }
}
