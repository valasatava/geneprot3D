package org.rcsb.exonscoassociation.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 5/3/17.
 */
public class TestResults {

    public static void main(String[] args){

        Dataset<Row> data = SaprkUtils.getSparkSession().read()
                .csv("/Users/yana/coassotiaded_exons/RESULTS/gencode.v24.CDS.protein_coding.exons_distances.csv");

//        data.show();

        Dataset<Row> filtered = data.filter( col("_c9").geq(50)
                .and(col("_c10").leq(5.0)).and(col("_c11").leq(15.0)).and(col("_c12").leq(15.0)));
        filtered.show();

        System.out.println(filtered.count());

    }
}
