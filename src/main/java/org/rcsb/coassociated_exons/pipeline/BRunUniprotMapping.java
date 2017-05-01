package org.rcsb.coassociated_exons.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Created by yana on 4/13/17.
 */
public class BRunUniprotMapping {

    public static void mapToUniprotPositions(String datapath, String mappingpath) {

        Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(datapath);
        data.persist(StorageLevel.MEMORY_AND_DISK());

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            Dataset<Row> map = MappingDataProvider.getHumanChromosomeMapping(chr);

            Dataset<Row> df1 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
                    .and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("start").equalTo(map.col("position"))), "inner")
                    .drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
                    .drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
                    .drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
                    .drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosStart").withColumnRenamed("uniProtCanonicalPos", "canonicalPosStart");

            Dataset<Row> df2 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
                    .and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("end").equalTo(map.col("position"))), "inner")
                    .drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
                    .drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
                    .drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
                    .drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosEnd").withColumnRenamed("uniProtCanonicalPos", "canonicalPosEnd");

            Dataset<Row> df = df1.join(df2,
                    df1.col("chromosome").equalTo(df2.col("chromosome"))
                            .and(df1.col("geneBankId").equalTo(df2.col("geneBankId")))
                            .and(df1.col("ensemblId").equalTo(df2.col("ensemblId")))
                            .and(df1.col("isoformIndex").equalTo(df2.col("isoformIndex")))
                            .and(df1.col("start").equalTo(df2.col("start")))
                            .and(df1.col("end").equalTo(df2.col("end"))), "inner")
                    .drop(df2.col("chromosome")).drop(df2.col("geneBankId")).drop(df2.col("ensemblId"))
                    .drop(df2.col("orientation")).drop(df2.col("offset")).drop(df2.col("geneName"))
                    .drop(df2.col("start")).drop(df2.col("end")).drop(df2.col("isoformIndex")).drop(df2.col("uniProtId"));

            Dataset<Row> ordered = df.select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
                    "uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd").orderBy("start", "isoformIndex");

            ordered.write().mode(SaveMode.Overwrite).parquet(mappingpath+"/"+chr);
        }
    }

    public static void runGencodeV24() throws Exception {
        mapToUniprotPositions(DataLocationProvider.getGencodeGeneBankLocation(),
                DataLocationProvider.getGencodeUniprotLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToUniprotPositions(DataLocationProvider.getExonsGeneBankLocation(),
                DataLocationProvider.getExonsUniprotLocation());
    }
}
