package org.rcsb.geneprot.transcriptomics.pipeline;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.transcriptomics.mapfunctions.MapToResolution;
import org.rcsb.geneprot.transcriptomics.utils.RowUtils;
import org.rcsb.geneprot.transcriptomics.filters.FilterBounaries;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import scala.Tuple2;

import java.util.List;

/**
 * Created by yana on 4/19/17.
 */
public class FMapToResolution {

    public static void map(String source, String dest) {

//        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr2"};

        for (String chr : chromosomes) {

            Dataset<Row> map = SparkUtils.getSparkSession().read().parquet(source + "/" + chr);
            JavaRDD<Row> extended = map.toJavaRDD().map(new MapToResolution());

            List<Tuple2<String, Iterable<Row>>> data = extended.map(new FilterBounaries()).filter(t -> (t != null))
                                                                .groupBy(t -> RowUtils.getExon(t))
                                                                .collect();

            for ( Tuple2<String, Iterable<Row>> t : data ) {
                System.out.println(t._1);
                System.out.println(t._2.toString());
            }
        }
    }

    public static void main(String[] args) {
        map(DataLocationProvider.getExonsStructuralMappingLocation(), "");
    }
}
