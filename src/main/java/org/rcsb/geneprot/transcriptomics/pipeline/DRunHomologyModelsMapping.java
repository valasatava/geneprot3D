package org.rcsb.geneprot.transcriptomics.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.geneprot.common.io.HomologyModelsProvider;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;

import static org.apache.spark.sql.functions.not;

/**
 * Created by yana on 4/13/17.
 */
public class DRunHomologyModelsMapping {

    /** The mapper class that map the homology models if they overlap with uniprot data ranges
     *
     * +----------+--------+---------------+------------+---------+---------+-----------+------+---------+-----------------+---------------+-----------+---------+--------+--------------------+
     |chromosome|geneName|      ensemblId|  geneBankId|    start|      end|orientation|offset|uniProtId|canonicalPosStart|canonicalPosEnd|fromUniprot|toUniprot|template|         coordinates|
     +----------+--------+---------------+------------+---------+---------+-----------+------+---------+-----------------+---------------+-----------+---------+--------+--------------------+
     |      chr1|   INTS3|ENST00000318967|NM_001324475|153760827|153760918|          +|     0|   Q68E01|              441|            471|         35|      499|4owt.1.A|https://swissmode...|
     |      chr1|   INTS3|ENST00000318967|   NM_023015|153760827|153760918|          +|     0|   Q68E01|              441|            471|         35|      499|4owt.1.A|https://swissmode...|
     |      chr1|  AKR1A1|ENST00000372070|NM_001202414| 45568485| 45568684|          +|     0|   P14550|              185|            251|          2|      325|1hqt.1.A|https://swissmode...|
     |      chr1|   BPNT1|ENST00000544404|NM_001286149|220072850|220072957|          -|     0|   O95861|              111|             76|          6|      308|2wef.1.A|https://swissmode...|
     |      chr1|    LRP8|ENST00000306052|   NM_004631| 53257240| 53257464|          -|     2|   Q14114|              812|            737|        302|      738|3p5c.1.C|https://swissmode...|
     *
     * @param mapToUniprot Dataset that maps genomic ranges to the Uniprot sequences
     * @param models homology models Dataset
     * @return Dataset that maps data ranges onto homology models ranges
     */
    public static Dataset<Row> mapToHomologyModels(Dataset<Row> mapToUniprot, Dataset<Row> models ) {

        Dataset<Row> mapToUniprotForward = mapToUniprot.filter(mapToUniprot.col("orientation").equalTo("+"));

        Dataset<Row> mapToHomologyForward = mapToUniprotForward.join(models,
                mapToUniprotForward.col("uniProtId").equalTo(models.col("uniProtId"))
                        .and( not((models.col("toUniprot").leq(mapToUniprotForward.col("canonicalPosStart"))
                                 .and(models.col("fromUniprot").leq(mapToUniprotForward.col("canonicalPosStart"))))
                             .or(models.col("fromUniprot").geq(mapToUniprotForward.col("canonicalPosEnd"))
                                 .and(models.col("toUniprot").geq(mapToUniprotForward.col("canonicalPosEnd")))))
                            ),
                "left")
                .drop(models.col("uniProtId")).drop(mapToUniprotForward.col("isoformIndex"))
                .drop(mapToUniprotForward.col("isoformPosStart")).drop(mapToUniprotForward.col("isoformPosEnd"))
                .filter(models.col("template").isNotNull());

        Dataset<Row> mapToUniprotReverse = mapToUniprot.filter(mapToUniprot.col("orientation").equalTo("-"));

        Dataset<Row> mapToHomologyReverse = mapToUniprotReverse.join(models,
                mapToUniprotReverse.col("uniProtId").equalTo(models.col("uniProtId"))
                    .and( not((models.col("toUniprot").leq(mapToUniprotReverse.col("canonicalPosEnd"))
                                    .and(models.col("fromUniprot").leq(mapToUniprotReverse.col("canonicalPosEnd"))))
                            .or(models.col("fromUniprot").geq(mapToUniprotReverse.col("canonicalPosStart"))
                                    .and(models.col("toUniprot").geq(mapToUniprotReverse.col("canonicalPosStart")))))
                    ),
                "left")
                .drop(models.col("uniProtId")).drop(mapToUniprotForward.col("isoformIndex"))
                .drop(mapToUniprotForward.col("isoformPosStart")).drop(mapToUniprotForward.col("isoformPosEnd"))
                .filter(models.col("template").isNotNull());

        Dataset<Row> mapToHomology = mapToHomologyForward.union(mapToHomologyReverse);

        return mapToHomology;
    }

    public static void mapToHomologyModels(String uniprotmapping, String mapping ) {

        Dataset<Row> models = HomologyModelsProvider.getAsDataFrame30pc(DataLocationProvider.getHomologyModelsLocation());

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chrX", "chrY", "chrM"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SparkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);

            Dataset<Row> mapToHomology = mapToHomologyModels( mapToUniprot, models );

            mapToHomology.write().mode(SaveMode.Overwrite).parquet(mapping+"/"+chr);
        }
    }

    public static void runGencode(String genomeName) throws Exception
    {
        DataLocationProvider.setGenome(genomeName);
        mapToHomologyModels(DataLocationProvider.getGencodeUniprotLocation(),
                DataLocationProvider.getGencodeHomologyMappingLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToHomologyModels(DataLocationProvider.getExonsUniprotLocation(),
                DataLocationProvider.getExonsHomologyModelsLocation());
    }


    public static void main(String[] args) {

        String coordinates = "https://swissmodel.expasy.org/repository/uniprot/Q9P2J2.pdb?range=27-505&template=5k6w.1.B&provider=swissmodel";

        Dataset<Row> models = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getHomologyModelsLocation());

        models.filter(models.col("coordinates").equalTo(coordinates)).show();

        models.filter(models.col("uniProtId").equalTo("Q9P2J2")).show();

    }
}
