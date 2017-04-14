package org.rcsb.correlatedexons.pipeline;

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

    public static void mapToPDBPositions(String uniprotmapping, String pdbmapping ) {

        Dataset<Row> mapUniprotToPdb = MappingDataProvider.getPdbUniprotMapping();

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr21"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);

            Dataset<Row> mapToPDBStart = mapToUniprot.join(mapUniprotToPdb,
                    mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                            .and(mapToUniprot.col("canonicalPosStart").equalTo(mapUniprotToPdb.col("uniProtPos"))))
                    .drop(mapUniprotToPdb.col("insCode"))
                    .drop(mapUniprotToPdb.col("uniProtId"))
                    .drop(mapUniprotToPdb.col("uniProtPos"))
                    .withColumnRenamed("pdbAtomPos","pdbPosStart");

            Dataset<Row> mapToPDBEnd = mapToUniprot.join(mapUniprotToPdb,
                    mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                            .and(mapToUniprot.col("canonicalPosEnd").equalTo(mapUniprotToPdb.col("uniProtPos"))))
                    .drop(mapUniprotToPdb.col("insCode"))
                    .drop(mapUniprotToPdb.col("uniProtId"))
                    .drop(mapUniprotToPdb.col("uniProtPos"))
                    .withColumnRenamed("pdbAtomPos","pdbPosEnd");

            Dataset<Row> mapToPDB = mapToPDBStart.join(mapToPDBEnd,
                    mapToPDBStart.col("ensemblId").equalTo(mapToPDBEnd.col("ensemblId"))
                            .and(mapToPDBStart.col("start").equalTo(mapToPDBEnd.col("start")))
                            .and(mapToPDBStart.col("end").equalTo(mapToPDBEnd.col("end")))
                            .and(mapToPDBStart.col("isoformIndex").equalTo(mapToPDBEnd.col("isoformIndex")))
                            .and(mapToPDBStart.col("pdbId").equalTo(mapToPDBEnd.col("pdbId")))
                            .and(mapToPDBStart.col("chainId").equalTo(mapToPDBEnd.col("chainId"))))
                    .drop(mapToPDBEnd.col("chromosome")).drop(mapToPDBEnd.col("geneName"))
                    .drop(mapToPDBEnd.col("ensemblId")).drop(mapToPDBEnd.col("geneBankId"))
                    .drop(mapToPDBEnd.col("start")).drop(mapToPDBEnd.col("end"))
                    .drop(mapToPDBEnd.col("orientation")).drop(mapToPDBEnd.col("offset"))
                    .drop(mapToPDBEnd.col("uniProtId")).drop(mapToPDBEnd.col("canonicalPosStart"))
                    .drop(mapToPDBEnd.col("canonicalPosEnd")).drop(mapToPDBEnd.col("isoformIndex"))
                    .drop(mapToPDBEnd.col("isoformPosStart")).drop(mapToPDBEnd.col("isoformPosEnd"))
                    .drop(mapToPDBEnd.col("pdbId")).drop(mapToPDBEnd.col("chainId"))
                    .select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
                            "uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd",
                            "pdbId","chainId","pdbPosStart","pdbPosEnd")
                    .orderBy("start", "isoformIndex");

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
