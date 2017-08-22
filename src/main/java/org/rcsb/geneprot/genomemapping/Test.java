package org.rcsb.geneprot.genomemapping;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.SparkUtils;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Yana Valasatava on 8/15/17.
 */
public class Test {

    public static void main(String[] args) {

        Dataset<Row> df = SparkUtils.getSparkSession().read().load("/Users/yana/spark/parquet/mousegenome/20170817/chr1");
        df.filter(col("inCoding").equalTo(true).and(col("position").equalTo(4492668))).sort("position", "isoformIndex").show(100);

    }
}
