package org.rcsb.geneprot.transcriptomics.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;

/**
 * Created by yana on 4/19/17.
 */
public class ShowDataframe {

    public static void main(String[] args) {

        String path = "/Users/yana/spark/parquet/mousegenome/20170905";

        DataLocationProvider.setGenome("mouse");
        Dataset<Row> d2 = SparkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getGencodeStructuralMappingLocation()+"/chr1").cache();
        d2.show();

        SparkUtils.stopSparkSession();
    }

}
