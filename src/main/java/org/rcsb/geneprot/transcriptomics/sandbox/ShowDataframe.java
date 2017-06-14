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

        Dataset<Row>  df = SaprkUtils.getSparkSession().read()
                .parquet("/Users/yana/data/parquet/dataframes.rcsb.org/parquet/humangenome/20170413/hg38/chr17");

        //df.show();

        df.createOrReplaceTempView("hgMapping");
        df.sqlContext().sql("SELECT chromosome, position, geneSymbol, uniProtId, " +
                "uniProtCanonicalPos, isoformIndex, uniProtIsoformPos FROM hgMapping " +
                "WHERE position IN (76091144, 76091235) ORDER BY position, isoformIndex").show();

        SaprkUtils.stopSparkSession();
    }
}
