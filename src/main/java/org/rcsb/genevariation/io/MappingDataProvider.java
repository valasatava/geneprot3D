package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;

public class MappingDataProvider {

	public static Dataset<Row> getHumanChromosomeMapping(String chr) {
        Dataset<Row> chrMapping = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getHgMappingLocation()+chr);
        return chrMapping;
	}
	
	public static Dataset<Row> getPdbUniprotMapping() {
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getUniprotPdbMappinlLocation());
		return mapping;
	}

	public static Dataset<Row> getHomologyModels() {
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

	public static void showCXADR() {
		Dataset<Row> chrom = getHumanChromosomeMapping("chr21");
		Dataset<Row> gene = chrom.filter(chrom.col("geneSymbol").equalTo("CXADR"))
				.filter(chrom.col("isoformIndex").equalTo(2))
				.filter(chrom.col("uniProtCanonicalPos").gt(0))
				.drop("geneBankId","position","mRNAPos").distinct()
				.orderBy(chrom.col("uniProtCanonicalPos"));
		gene.show(100);
	}

	public static void main(String[] args) {

		Dataset<Row> up = SaprkUtils.getSparkSession()
				.read().parquet(DataLocationProvider.getExonsUniprotLocation()+"/chr21");
		System.out.println(up.count());

		Dataset<Row> mp = SaprkUtils.getSparkSession()
				.read().parquet(DataLocationProvider.getExonsPDBLocation()+"/chr21");
		System.out.println(mp.count());
		mp.show(100);

	}
}
