package org.rcsb.correlatedexons.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Created by yana on 4/13/17.
 */
public class EGetStructuralMapping {

    public static void combinePDBStructuresAndHomologyModels(String pdbMapping, String homologyMapping, String structuralMapping) {

        Dataset<Row> mapUniprotToPdb = MappingDataProvider.getPdbUniprotMapping();
        mapUniprotToPdb.persist();

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToPDB = SaprkUtils.getSparkSession().read().parquet(pdbMapping + "/" + chr);
            Dataset<Row> pdbs = mapToPDB.filter("pdbId is not null");

            pdbs.filter(pdbs.col("geneName").equalTo("BROX")).show();

            pdbs.filter(pdbs.col("geneName").equalTo("BROX").and(pdbs.col("start").equalTo(222731357))).show();

            Dataset<Row> df1 = pdbs//.filter(pdbs.col("uniProtId").equalTo("Q5VW32"))
                    .withColumn("template", functions.lit("null"))
                    .withColumn("coordinates", functions.lit("null"))
                    .drop(pdbs.col("isoformIndex")).drop(pdbs.col("isoformPosStart")).drop(pdbs.col("isoformPosEnd"));

            Dataset<Row> mapToModels = SaprkUtils.getSparkSession().read().parquet(homologyMapping + "/" + chr);
            Dataset<Row> models = mapToModels.filter("template is not null");

            Dataset<Row> df2 = models//.filter(models.col("uniProtId").equalTo("Q5VW32"))
                    .drop(models.col("fromUniprot")).drop(models.col("toUniprot"))
                    .drop(models.col("fromPdb")).drop(models.col("toPdb"))
                    .drop(models.col("isoformIndex")).drop(models.col("isoformPosStart")).drop(models.col("isoformPosEnd"))
                    .withColumn("pdbId", functions.lit("null"))
                    .withColumn("chainId", functions.lit("null"))
                    .withColumn("pdbPosStart", functions.lit("null"))
                    .withColumn("pdbPosEnd", functions.lit("null"))
                    .select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end",
                            "orientation", "offset", "uniProtId", "canonicalPosStart", "canonicalPosEnd",
                            "pdbId", "chainId", "pdbPosStart", "pdbPosEnd", "template", "coordinates");

            Dataset<Row> df3 = df1.union(df2).orderBy("start","end");
            df3.write().mode(SaveMode.Overwrite).parquet(structuralMapping+"/"+chr);
        }
    }

    public static void runGencodeV24() throws Exception { }

    public static void runCorrelatedExons() throws Exception {
        combinePDBStructuresAndHomologyModels(DataLocationProvider.getExonsPDBLocation(),
                DataLocationProvider.getExonsHomologyModelsLocation(), DataLocationProvider.getExonsStructuralMappingLocation());
    }
}
