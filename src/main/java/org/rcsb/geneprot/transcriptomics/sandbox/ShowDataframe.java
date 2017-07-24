package org.rcsb.geneprot.transcriptomics.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 4/19/17.
 */
public class ShowDataframe {

    public static void main(String[] args) {

        Dataset<Row>  df2 = SaprkUtils.getSparkSession().read()
                .format("com.databricks.spark.csv")
                .option("delimiter", "\t")
                .option("header", "true")
                .load("/Users/yana/Projects/mmtf_workshop/oncokb_variants_missense.txt");
        df2.show();

        SaprkUtils.stopSparkSession();
    }
}
