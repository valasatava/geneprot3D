package org.rcsb.geneprot.transcriptomics.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.SaprkUtils;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 4/19/17.
 */
public class ShowDataframe {

    public static void main(String[] args) {

        Dataset<Row>  d = SaprkUtils.getSparkSession().read()
                .parquet("/Users/yana/spark/parquet/uniprotpdb/20170629");
        d.filter(col("pdbId").equalTo("5IRC").and(col("chainId").equalTo("F"))).show();

        SaprkUtils.stopSparkSession();
    }
}
