package org.rcsb.genevariation.dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;

import static org.apache.spark.sql.functions.upper;

/**
 * Created by yana on 4/13/17.
 */
public class CreateHomologyMapping {

    public static void runGood(String path) throws Exception {

        Dataset<Row> models = MappingDataProvider.getHomologyModels();
        models.show();

        Dataset<Row> homologues = models.select("uniProtId", "fromPos", "toPos", "similarity", "template", "coordinates")
                .filter(models.col("similarity").gt(0.3))
                .withColumnRenamed("fromPos", "fromUniprot")
                .withColumnRenamed("toPos", "toUniprot")
                .drop(models.col("similarity"));

        homologues.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public static void runMapping(String path) throws Exception {

        Dataset<Row> models = MappingDataProvider.getHomologyModels();
        
        Dataset<Row> homologues = models.select("uniProtId", "fromPos", "toPos", "similarity", "template", "coordinates")
                .filter(models.col("similarity").gt(0.3))
                .withColumnRenamed("fromPos", "fromUniprot")
                .withColumnRenamed("toPos", "toUniprot")
                .drop(models.col("similarity"));

        Dataset<Row> mapUniprotToPdb = MappingDataProvider.getPdbUniprotMapping();

        Dataset<Row> mappingStart = homologues.join(mapUniprotToPdb,
                homologues.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(homologues.col("fromUniprot").equalTo(mapUniprotToPdb.col("uniProtPos")))
                        .and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("pdbId")))
                        .and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("chainId")))
        )
                .drop(mapUniprotToPdb.col("insCode"))
                .drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos"))
                .withColumnRenamed("pdbAtomPos", "fromPdb");

        Dataset<Row> mappingEnd = homologues.join(mapUniprotToPdb,
                homologues.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(homologues.col("toUniprot").equalTo(mapUniprotToPdb.col("uniProtPos")))
                        .and(upper(homologues.col("template")).contains(mapUniprotToPdb.col("pdbId")))
                        .and(homologues.col("template").contains(mapUniprotToPdb.col("chainId")))
        )
                .drop(mapUniprotToPdb.col("insCode"))
                .drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos"))
                .withColumnRenamed("pdbAtomPos", "toPdb");

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

        mapping.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public static void main(String[] args) throws Exception {

        long start = System.nanoTime();

        runGood(DataLocationProvider.getHumanGoodHomologyModelsLocation());

        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }
}
