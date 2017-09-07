package org.rcsb.geneprot.transcriptomics.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genes.datastructures.ExonSerializable;
import org.rcsb.geneprot.transcriptomics.utils.ExonsUtils;

import java.util.List;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 4/13/17.
 */
public class BRunUniprotMapping {

    public static void mapToUniprotPositions(String datapath, String mappingpath) {

        //Dataset<Row> data = SparkUtils.getSparkSession().read().parquet(datapath);

        DataLocationProvider.setGenome("mouse");
        List<ExonSerializable> exons = ExonsUtils.getSerializableExons(DataLocationProvider.getGencodeProteinCodingDataLocation());
        Dataset<Row> data = SparkUtils.getSparkSession().createDataFrame(exons, ExonSerializable.class);
        data.persist(StorageLevel.MEMORY_AND_DISK());

        //data.filter(col("ensemblId").equalTo("ENSMUST00000114513")).sort(col("start")).show();

//        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19","chrX", "chrY", "chrM"};

        SparkUtils.getSparkSession().udf().register("major_version", (String x) -> x.split(Pattern.quote("."))[0], DataTypes.StringType);

        for (String chr : chromosomes) {

            //Dataset<Row> map = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getHgMappingLocation()+chr);
            Dataset<Row> m = SparkUtils.getSparkSession().read().parquet("/Users/yana/spark/parquet/mousegenome/20170905/"+chr);
            Dataset<Row> map = m.withColumn("tmp", callUDF("major_version", col("geneBankId")))
                    .withColumnRenamed("tmp", "ensemblId");
            //map.filter(col("ensemblId").equalTo("ENSMUST00000114513").and(col("position").between(52232582, 52232592))).sort(col("position")).show();

            Dataset<Row> df1 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
                    .and(data.col("ensemblId").equalTo(map.col("ensemblId"))).and(data.col("start").equalTo(map.col("position"))), "inner")
                    .drop(map.col("chromosome")).drop(map.col("ensemblId")).drop(map.col("geneBankId")).drop(map.col("orientation"))
                    .drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
                    .drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
                    .drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosStart").withColumnRenamed("uniProtCanonicalPos", "canonicalPosStart");
            //df1.filter(col("ensemblId").equalTo("ENSMUST00000114513")).show();

            Dataset<Row> df2 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
                    .and(data.col("ensemblId").equalTo(map.col("ensemblId"))).and(data.col("end").equalTo(map.col("position"))), "inner")
                    .drop(map.col("chromosome")).drop(map.col("ensemblId")).drop(map.col("geneBankId")).drop(map.col("orientation"))
                    .drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
                    .drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
                    .drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosEnd").withColumnRenamed("uniProtCanonicalPos", "canonicalPosEnd");
            //df2.filter(col("ensemblId").equalTo("ENSMUST00000114513")).show();

            Dataset<Row> df = df1.join(df2, df1.col("chromosome").equalTo(df2.col("chromosome"))
                            .and(df1.col("ensemblId").equalTo(df2.col("ensemblId")))
                            .and(df1.col("isoformIndex").equalTo(df2.col("isoformIndex")))
                            .and(df1.col("start").equalTo(df2.col("start")))
                            .and(df1.col("end").equalTo(df2.col("end"))), "inner")
                    .drop(df2.col("chromosome")).drop(df2.col("geneBankId")).drop(df2.col("ensemblId"))
                    .drop(df2.col("orientation")).drop(df2.col("offset")).drop(df2.col("geneName"))
                    .drop(df2.col("start")).drop(df2.col("end")).drop(df2.col("isoformIndex")).drop(df2.col("uniProtId"));
            //df.filter(col("ensemblId").equalTo("ENSMUST00000114513")).show();

            Dataset<Row> ordered = df.select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
                    "uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd").orderBy("start", "isoformIndex");

            ordered.write().mode(SaveMode.Overwrite).parquet(mappingpath+"/"+chr);
        }
    }

    public static void runGencode(String genomeName) throws Exception
    {
        DataLocationProvider.setGenome(genomeName);
        mapToUniprotPositions(DataLocationProvider.getGencodeGeneBankLocation(),
                DataLocationProvider.getGencodeUniprotLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToUniprotPositions(DataLocationProvider.getExonsGeneBankLocation(),
                DataLocationProvider.getExonsUniprotLocation());
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("Uniprot mapping...");
        BRunUniprotMapping.runGencode("mouse");
        System.out.println("...done!");
    }
}
