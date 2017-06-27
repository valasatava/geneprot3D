package org.rcsb.geneprot.common.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.split;

import org.rcsb.geneprot.common.utils.SaprkUtils;

/**
 * Created by Yana Valasatava on 6/27/17.
 */
public class Test {

    public static void main(String[] args) throws Exception {

//        Dataset<Row> gene2acc = SaprkUtils.getSparkSession()
//                .read().format("com.databricks.spark.csv")
//                .option("delimiter", "\t").option("header", "true")
//                .load("/Users/yana/Downloads/gene2accession")
//                .withColumnRenamed("`protein_accession.version`", "protein_accession")
//                .select(col("GeneID"), split(col("protein_accession"), "\\."))
//                .filter(not(col("protein_accession").equalTo("-")))
//                .filter(not(col("protein_accession").contains("_")));
//        gene2acc.show();

        System.out.println("ok");

    }
}
