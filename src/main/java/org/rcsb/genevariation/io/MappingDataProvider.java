package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.utils.SaprkUtils;

import static org.apache.spark.sql.functions.upper;

public class MappingDataProvider {

	public static Dataset<Row> readHumanChromosomeMapping(String chr) {
        Dataset<Row> chrMapping = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getHgMappingLocation()+chr);
        return chrMapping;
	}
	
	public static Dataset<Row> readPdbUniprotMapping() {
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getUniprotPdbMappinlLocation());
		return mapping;
	}

	public static Dataset<Row> readHomologyModels() {
		Dataset<Row> models = SaprkUtils.getSparkSession()
				.read()
				.parquet(DataLocationProvider.getHumanHomologyModelsLocation());
		return models;
	}

	public static Dataset<Row> getGeneBankToEnsembleMapping() {
		// Ensembl to gene bank id mapping
		Dataset<Row> mp = SaprkUtils.getSparkSession()
				.read().csv(DataLocationProvider.getExonsProject()+"MAPS/mart_export.txt")
				.filter(t->t.getAs(1)!=null)
				.withColumnRenamed("_c0", "ensemblId")
				.withColumnRenamed("_c1", "geneBankId");
		return mp;
	}
}
