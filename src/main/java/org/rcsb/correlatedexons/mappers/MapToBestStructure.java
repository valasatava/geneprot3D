package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.ReaderUtils;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by yana on 4/17/17.
 */
public class MapToBestStructure implements PairFunction<String, String, Row> {

    private Broadcast<Dataset<Row>> mbc = null;

    public MapToBestStructure(Broadcast<Dataset<Row>> mapbc) {
        mbc = mapbc;
    }

    @Override
    public Tuple2<String, Row> call(String key) throws Exception {

        Dataset<Row> map = mbc.value();

//        if (key.equals("222731357_222731516_1"))
//            System.out.println();

        String start = key.split("_")[0];
        String end = key.split("_")[1];
        String offset = key.split("_")[2];

        Dataset<Row> exon = map.filter(map.col("start").equalTo(Integer.valueOf(start))
                .and(map.col("end").equalTo(Integer.valueOf(end)))
                .and(map.col("offset").equalTo(Integer.valueOf(offset))));
        List<Row> rows = exon.collectAsList();

        Float bestres = 99.9f;
        Row bestrow = null;

        for ( Row row : rows ) {

            String pdbId = row.getString(11);

            if (pdbId.equals("null")) {
                pdbId = row.getString(15).split(Pattern.quote("."))[0];
            }

            MmtfStructure mmtfData = ReaderUtils.getDataFromUrl(pdbId);
            Float resolution = mmtfData.getResolution();

            if (resolution == null)
                continue;

            if (resolution < bestres) {
                bestres = resolution;
                bestrow = row;
            }
        }

        if (bestres == null)
            return null;

        return new Tuple2<String, Row>(key, bestrow);
    }
}
