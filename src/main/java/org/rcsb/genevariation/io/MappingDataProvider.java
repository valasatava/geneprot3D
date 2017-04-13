package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.utils.SaprkUtils;

import static org.apache.spark.sql.functions.upper;

public class MappingDataProvider extends DataLocationProvider {
	
	private final static String dfGenevariationPath = getUserHome() + "/spark/parquet/humangenome/20170405/hg38/";
	private final static String dfUniprotpdbPath = getDataHome() + "/parquet/uniprotpdb/20161104/";
	private final static String dfHomologyModelsPath = getDataHome() + "/parquet/human-homology-models";

	public static Dataset<Row> readHumanChromosomeMapping(String chr) {
        Dataset<Row> chrMapping = SaprkUtils.getSparkSession().read().parquet(dfGenevariationPath+chr);
        return chrMapping;
	}
	
	public static Dataset<Row> readPdbUniprotMapping() {
		Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(dfUniprotpdbPath);
		return mapping;
	}

	public static void mapGoodHomologyModels(String path) throws Exception {

		Dataset<Row> models = SaprkUtils.getSparkSession().read().parquet(dfHomologyModelsPath);

		Dataset<Row> homologues = models.select("uniProtId", "fromPos", "toPos", "similarity", "template", "coordinates")
				.filter(models.col("similarity").gt(0.3))
				.withColumnRenamed("fromPos", "fromUniprot")
				.withColumnRenamed("toPos", "toUniprot")
				.drop(models.col("similarity"));

		Dataset<Row> mapUniprotToPdb = readPdbUniprotMapping();

		Dataset<Row> mappingStart = homologues.join(mapUniprotToPdb,
				homologues.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
					.and(homologues.col("fromUniprot").equalTo(mapUniprotToPdb.col("uniProtPos")))
					.and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("pdbId")))
					.and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("chainId")))
				)
				.drop(mapUniprotToPdb.col("insCode"))
				.drop(mapUniprotToPdb.col("uniProtId"))
				.drop(mapUniprotToPdb.col("uniProtPos"))
				.withColumnRenamed("pdbAtomPos","fromPdb");

		Dataset<Row> mappingEnd = homologues.join(mapUniprotToPdb,
				homologues.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
					.and(homologues.col("toUniprot").equalTo(mapUniprotToPdb.col("uniProtPos")))
					.and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("pdbId")))
					.and(homologues.col("template").contains(mapUniprotToPdb.col("chainId")))
				)
				.drop(mapUniprotToPdb.col("insCode"))
				.drop(mapUniprotToPdb.col("uniProtId"))
				.drop(mapUniprotToPdb.col("uniProtPos"))
				.withColumnRenamed("pdbAtomPos","toPdb");

		Dataset<Row> mapping = mappingStart.join(mappingEnd,
				mappingStart.col("uniProtId").equalTo(mappingEnd.col("uniProtId"))
					.and(mappingStart.col("fromUniprot").equalTo(mappingEnd.col("fromUniprot")))
					.and(mappingStart.col("toUniprot").equalTo(mappingEnd.col("toUniprot")))
					.and(mappingStart.col("template").equalTo(mappingEnd.col("template")))
					.and(mappingStart.col("pdbId").equalTo(mappingEnd.col("pdbId")))
					.and(mappingStart.col("chainId").equalTo(mappingEnd.col("chainId")))
				)
				.drop(mappingEnd.col("uniProtId")).drop(mappingEnd.col("fromUniprot"))
				.drop(mappingEnd.col("toUniprot")).drop(mappingEnd.col("template"))
				.drop(mappingEnd.col("coordinates")).drop(mappingEnd.col("pdbId"))
				.drop(mappingEnd.col("chainId"))
				.select("uniProtId", "fromUniprot", "toUniprot", "template", "coordinates", "pdbId", "chainId", "fromPdb", "toPdb");

//		mapping//.filter(mapping.col("pdbId").equalTo("2Z7C").and(mapping.col("chainId").equalTo("B")))
//				.show();

		mapping.write().mode(SaveMode.Overwrite).parquet(path);
	}

	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();

		String path = "/Users/yana/data/genevariation/parquet/homology-models-mapping-pc30";
		mapGoodHomologyModels(path);

		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
