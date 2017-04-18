package org.rcsb.correlatedexons.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.HomologyModelsProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Created by yana on 4/13/17.
 */
public class DRunHomologyModelsMapping {

    public static void mapToHomologyModels(String uniprotmapping, String mapping ) {

        Dataset<Row> models = HomologyModelsProvider.get30pcAsDataFrame();

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);

            Dataset<Row> mapToUniprotForward = mapToUniprot.filter(mapToUniprot.col("orientation").equalTo("+"));

            Dataset<Row> mapToHomologyForward = mapToUniprotForward.join(models,
                    mapToUniprotForward.col("uniProtId").equalTo(models.col("uniProtId"))
                    .and(mapToUniprotForward.col("canonicalPosStart").geq(models.col("fromUniprot"))
                            .and(mapToUniprotForward.col("canonicalPosEnd").leq(models.col("toUniprot")))),
                    "left")
                    .drop(models.col("uniProtId")).drop(models.col("pdbId")).drop(models.col("chainId"));

            Dataset<Row> mapToUniprotReverse = mapToUniprot.filter(mapToUniprot.col("orientation").equalTo("-"));

            Dataset<Row> mapToHomologyReverse = mapToUniprotReverse.join(models,
                    mapToUniprotReverse.col("uniProtId").equalTo(models.col("uniProtId"))
                    .and(mapToUniprotReverse.col("canonicalPosStart").geq(models.col("toUniprot")))
                            .and(mapToUniprotReverse.col("canonicalPosEnd").leq(models.col("fromUniprot"))),
                    "left")
                    .drop(models.col("uniProtId")).drop(models.col("pdbId")).drop(models.col("chainId"));

            Dataset<Row> mapToHomology = mapToHomologyForward.union(mapToHomologyReverse);

            mapToHomology.write().mode(SaveMode.Overwrite).parquet(mapping+"/"+chr);

        }
    }

    public static void runGencodeV24() throws Exception {
        mapToHomologyModels(DataLocationProvider.getGencodeUniprotLocation(),
                DataLocationProvider.getHomologyModelsMappingLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToHomologyModels(DataLocationProvider.getExonsUniprotLocation(),
                DataLocationProvider.getExonsHomologyModelsLocation());
    }
}
