package org.rcsb.geneprot.common.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.SparkUtils;

public class MappingDataProvider {

	public static Dataset<Row> getGeneBankToEnsembleMapping() {
		// Ensembl to gene bank id mapping
		Dataset<Row> mp = SparkUtils.getSparkSession()
				.read().csv(DataLocationProvider.getExonsProject()+"MAPS/mart_export.txt")
				.filter(t->t.getAs(1)!=null)
				.withColumnRenamed("_c0", "ensemblId")
				.withColumnRenamed("_c1", "geneBankId");
		return mp;
	}
}
