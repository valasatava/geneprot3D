package org.rcsb.geneprot.transcriptomics.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;

/**
 * Created by yana on 4/19/17.
 */
public class ShowDataframe {

    public static void main(String[] args) {

//        Dataset<Row>  d1 = SaprkUtils.getSparkSession().read()
//                .parquet(DataLocationProvider.getHgMappingLocation()+"/chr19");
//        d1.filter(col("geneSymbol").equalTo("CARM1")).select(col("geneBankId"), col("isoformIndex"))
//                .distinct().show();

        Dataset<Row> d2 = SaprkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getUniprotPdbMappinlLocation());
        d2.show();

        SaprkUtils.stopSparkSession();
    }

}
