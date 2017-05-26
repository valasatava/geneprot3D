package org.rcsb.geneprot.transcriptomics.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.SaprkUtils;

/**
 * Created by yana on 4/19/17.
 */
public class ShowDataframe {

    public static void main(String[] args) {

        Dataset<Row>  d = SaprkUtils.getSparkSession().read()
                .parquet("/Users/yana/data/genevariation/parquet/human-homology-models");
        d.show();

        SaprkUtils.stopSparkSession();
    }
}
