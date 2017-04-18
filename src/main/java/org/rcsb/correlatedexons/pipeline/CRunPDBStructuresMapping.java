package org.rcsb.correlatedexons.pipeline;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

/**
 * Created by yana on 4/13/17.
 */
public class CRunPDBStructuresMapping {

    public static Dataset<Row>  mapToPDBPositions(Dataset<Row> mapUniprotToPdb, Dataset<Row> mapToUniprot ) throws AnalysisException {

        Dataset<Row> mapToPDBStart = mapToUniprot.join(mapUniprotToPdb,
                mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(mapToUniprot.col("canonicalPosStart").equalTo(mapUniprotToPdb.col("uniProtPos"))),"left")
                .drop(mapUniprotToPdb.col("insCode"))
                .drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos"))
                .drop(mapUniprotToPdb.col("isoformIndex"))
                .drop(mapUniprotToPdb.col("isoformPosStart"))
                .drop(mapUniprotToPdb.col("isoformPosEnd"))
                .withColumnRenamed("pdbAtomPos","pdbPosStart");


        Dataset<Row> df1 = mapToPDBStart.filter(mapToPDBStart.col("geneName").equalTo("BROX").and(mapToPDBStart.col("start").equalTo(222731357)));
        df1.show();

        Dataset<Row> mapToPDBEnd = mapToUniprot.join(mapUniprotToPdb,
                mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(mapToUniprot.col("canonicalPosEnd").equalTo(mapUniprotToPdb.col("uniProtPos"))), "left")
                .drop(mapUniprotToPdb.col("insCode"))
                .drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos"))
                .drop(mapUniprotToPdb.col("isoformIndex"))
                .drop(mapUniprotToPdb.col("isoformPosStart"))
                .drop(mapUniprotToPdb.col("isoformPosEnd"))
                .withColumnRenamed("pdbAtomPos","pdbPosEnd");

        Dataset<Row> df2 = mapToPDBEnd.filter(mapToPDBEnd.col("geneName").equalTo("BROX").and(mapToPDBEnd.col("end").equalTo(222731516)));
        df2.show();

        Dataset<Row> df3 = df1.join(df2, df1.col("start").equalTo(df2.col("start")).and(df1.col("end").equalTo(df2.col("end"))), "fullouter");

//        Dataset<Row> df3 = SaprkUtils.getSparkSession().sql("select df1.chromosome, df1.geneName, df1.ensemblId, df1.geneBankId, " +
//                "df1.start, df1.end, df1.orientation, df1.offset, df1.uniProtId, df1.canonicalPosStart, df1.canonicalPosEnd," +
//                "df1.pdbId, df1.chainId, df1.pdbPosStart, df2.pdbPosEnd from df1 FULL OUTER JOIN df2 on (df1.start=df2.start and df1.end=df2.end and df1.offset=df2.offset) " +
//                "where ( ( (df1.pdbId=df2.pdbId and df1.chainId=df2.chainId) or df1.pdbPosStart is null) or ( (df1.pdbId=df2.pdbId and df1.chainId=df2.chainId) or df2.pdbPosEnd is null) )");
        df3.show();

        Dataset<Row> mapToPDB = mapToPDBStart.join(mapToPDBEnd,
                mapToPDBStart.col("ensemblId").equalTo(mapToPDBEnd.col("ensemblId"))
                        .and(mapToPDBStart.col("start").equalTo(mapToPDBEnd.col("start")))
                        .and(mapToPDBStart.col("end").equalTo(mapToPDBEnd.col("end")))
                        .and(mapToPDBStart.col("isoformIndex").equalTo(mapToPDBEnd.col("isoformIndex")))
                        .and(mapToPDBStart.col("pdbId").isNull().or(mapToPDBEnd.col("pdbId").isNull().or(mapToPDBStart.col("pdbId").equalTo(mapToPDBEnd.col("pdbId"))
                                .and(mapToPDBStart.col("chainId").equalTo(mapToPDBEnd.col("chainId"))))))

,
                "full")
                .drop(mapToPDBEnd.col("chromosome")).drop(mapToPDBEnd.col("geneName"))
                .drop(mapToPDBEnd.col("ensemblId")).drop(mapToPDBEnd.col("geneBankId"))
                .drop(mapToPDBEnd.col("start")).drop(mapToPDBEnd.col("end"))
                .drop(mapToPDBEnd.col("orientation")).drop(mapToPDBEnd.col("offset"))
                .drop(mapToPDBEnd.col("uniProtId")).drop(mapToPDBEnd.col("canonicalPosStart"))
                .drop(mapToPDBEnd.col("canonicalPosEnd")).drop(mapToPDBEnd.col("isoformIndex"))
                .drop(mapToPDBEnd.col("isoformPosStart")).drop(mapToPDBEnd.col("isoformPosEnd"))
                .drop(mapToPDBEnd.col("pdbId")).drop(mapToPDBEnd.col("chainId"))
                .filter("chromosome is not null")
                .select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
                        "uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd",
                        "pdbId","chainId","pdbPosStart","pdbPosEnd")
                .distinct()
                .orderBy("geneName", "isoformIndex", "start", "end");

        mapToPDB.filter(mapToPDB.col("geneName").equalTo("BROX").and(mapToPDB.col("start").equalTo(222731357)).and(mapToPDB.col("end").equalTo(222731516))).show();

        return mapToPDB;
    }

    public static void mapToPDBPositions(String uniprotmapping, String pdbmapping ) throws AnalysisException {

        Dataset<Row> mapUniprotToPdb = MappingDataProvider.getPdbUniprotMapping();

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);
            Dataset<Row> mapToPDB = mapToPDBPositions(mapUniprotToPdb, mapToUniprot);
            mapToPDB.write().mode(SaveMode.Overwrite).parquet(pdbmapping+"/"+chr);
        }
    }

    public static void runGencodeV24() throws Exception {
        mapToPDBPositions(DataLocationProvider.getGencodeUniprotLocation(),
                DataLocationProvider.getGencodePDBLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToPDBPositions(DataLocationProvider.getExonsUniprotLocation(),
                DataLocationProvider.getExonsPDBLocation());
    }
}
