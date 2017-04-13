package org.rcsb.correlatedexons.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.HomologyModelsProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Created by yana on 4/13/17.
 */
public class DRunHomologyModelsMapping {

    public static void mapToHomologyModels(String uniprotmapping, String mapping ) {

        Dataset<Row> models = HomologyModelsProvider.getAsDataFrame();
        models.sort(models.col("template")).show();

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr21"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);
            mapToUniprot.show();

            Dataset<Row> mapToHomology = mapToUniprot.join(models,
                    mapToUniprot.col("uniProtId").equalTo(models.col("uniProtId"))
                            .and(
                                    mapToUniprot.col("canonicalPosStart").geq(models.col("fromUniprot"))
                                            .and(mapToUniprot.col("canonicalPosEnd").leq(models.col("toUniprot")))
                                            .and(mapToUniprot.col("orientation").equalTo("+"))
                                            .or(mapToUniprot.col("canonicalPosStart").geq(models.col("toUniprot"))
                                                    .and(mapToUniprot.col("canonicalPosEnd").leq(models.col("fromUniprot")))
                                                    .and(mapToUniprot.col("orientation").equalTo("-")))
                            )
            )
                    .drop(models.col("uniProtId"));
            mapToHomology.show();
        }
    }

    public static void runGencodeV24() throws Exception {
        mapToHomologyModels(DataLocationProvider.getGencodeUniprotLocation(),
                DataLocationProvider.getHomologyModelsMappingLocation());
    }
}
