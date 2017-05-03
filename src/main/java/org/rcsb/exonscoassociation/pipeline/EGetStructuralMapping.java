package org.rcsb.exonscoassociation.pipeline;

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

            Dataset<Row> pdbs = SaprkUtils.getSparkSession().read().parquet(pdbMapping + "/" + chr);
            Dataset<Row> df1 = pdbs
                    .withColumn("template", functions.lit("null"))
                    .withColumn("coordinates", functions.lit("null"))
                    .select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end",
                            "orientation", "offset", "uniProtId", "canonicalPosStart", "canonicalPosEnd",
                            "pdbId", "chainId", "pdbPosStart", "pdbPosEnd", "template", "coordinates");

            Dataset<Row> models = SaprkUtils.getSparkSession().read().parquet(homologyMapping + "/" + chr);
            Dataset<Row> df2 = models
                    .withColumnRenamed("fromUniprot", "pdbPosStart")
                    .withColumnRenamed("toUniprot", "pdbPosEnd")
                    .withColumn("pdbId", functions.lit("null")).withColumn("chainId", functions.lit("null"))
                    .select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end",
                            "orientation", "offset", "uniProtId", "canonicalPosStart", "canonicalPosEnd",
                            "pdbId", "chainId", "pdbPosStart", "pdbPosEnd", "template", "coordinates");

            Dataset<Row> df3 = df1.union(df2).orderBy("start","end");

            df3.write().mode(SaveMode.Overwrite).parquet(structuralMapping+"/"+chr);
        }
    }

    public static void runGencodeV24() throws Exception {
        combinePDBStructuresAndHomologyModels(DataLocationProvider.getGencodePDBLocation(),
                DataLocationProvider.getGencodeHomologyModelsLocation(),
                DataLocationProvider.getGencodeStructuralMappingLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        combinePDBStructuresAndHomologyModels(DataLocationProvider.getExonsPDBLocation(),
                DataLocationProvider.getExonsHomologyModelsLocation(),
                DataLocationProvider.getExonsStructuralMappingLocation());
    }

    public static void main(String[] args) {

        Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getExonsStructuralMappingLocation() + "/chr1");
        mapping.orderBy("start", "end", "pdbId", "chainId").show(100);
    }
}
