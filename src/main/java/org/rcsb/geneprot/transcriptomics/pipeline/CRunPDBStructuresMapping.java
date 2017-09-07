package org.rcsb.geneprot.transcriptomics.pipeline;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/13/17.
 */
public class CRunPDBStructuresMapping {

    /** The mapper class to to Uniprot sequences ranges and PDB structures ranges
     *+----------+--------+---------------+------------+-----------+---------+---------+---------+------+-----------------+---------------+-----+-------+-----------+---------+
     |chromosome|geneName|      ensemblId|  geneBankId|orientation|uniProtId|    start|      end|offset|canonicalPosStart|canonicalPosEnd|pdbId|chainId|pdbPosStart|pdbPosEnd|
     +----------+--------+---------------+------------+-----------+---------+---------+---------+------+-----------------+---------------+-----+-------+-----------+---------+
     |     chr12|   DNM1L|ENST00000452533|   NM_012063|          +|   O00429| 32720664| 32720795|     1|              247|            291| 4H1V|      A|        247|      291|
     |     chr12|    FRS2|ENST00000549921|NM_001278351|          +|   Q8WU20| 69570331| 69570517|     0|               23|             85| 2MFQ|      A|         23|       85|
     |     chr12|   DNM1L|ENST00000452533|   NM_012063|          +|   O00429| 32733715| 32733807|     0|              483|            513| 4BEJ|      A|        483|     null|
     *
     * @param mapUniprotToPdb Dataset that maps genomic ranges to Uniprot sequences onto PDB structures
     * @param mapToUniprot Dataset that maps genomic ranges to the Uniprot sequences
     * @return Dataset that maps the genomic ranges to Uniprot sequences ranges and PDB structures ranges
     *
     * @throws AnalysisException
     */
    public static Dataset<Row>  mapToPDBPositions(Dataset<Row> mapUniprotToPdb, Dataset<Row> mapToUniprot ) throws AnalysisException {

        Dataset<Row> mapToPDBStart = mapToUniprot.join(mapUniprotToPdb,
                mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(mapToUniprot.col("canonicalPosStart").equalTo(mapUniprotToPdb.col("uniProtPos"))),"left")
                .drop(mapUniprotToPdb.col("insCode")).drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos")).drop(mapToUniprot.col("isoformIndex"))
                .drop(mapToUniprot.col("isoformPosStart")).drop(mapToUniprot.col("isoformPosEnd"))
                .withColumnRenamed("pdbAtomPos","pdbPosStart");

        Dataset<Row> mapToPDBEnd = mapToUniprot.join(mapUniprotToPdb,
                mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
                        .and(mapToUniprot.col("canonicalPosEnd").equalTo(mapUniprotToPdb.col("uniProtPos"))), "left")
                .drop(mapUniprotToPdb.col("insCode")).drop(mapUniprotToPdb.col("uniProtId"))
                .drop(mapUniprotToPdb.col("uniProtPos")).drop(mapToUniprot.col("isoformIndex"))
                .drop(mapToUniprot.col("isoformPosStart")).drop(mapToUniprot.col("isoformPosEnd"))
                .withColumnRenamed("pdbAtomPos","pdbPosEnd");

        List<String> columnnames = new ArrayList<String>(){{
            add("chromosome");add("geneName");add("ensemblId");add("geneBankId");add("orientation");add("uniProtId");
            add("start");add("end");add("offset");add("canonicalPosStart");add("canonicalPosEnd");
            add("pdbId");add("chainId");
        }};
        Seq<String> columns = JavaConversions.asScalaBuffer(columnnames).toSeq();
        Dataset<Row> mapToPDB = mapToPDBStart.join(mapToPDBEnd, columns, "outer").filter("pdbId is not null");

        return mapToPDB;
    }

    public static void mapToPDBPositions(String uniprotmapping, String pdbmapping ) throws AnalysisException {

        Dataset<Row> mapUniprotToPdb = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getUniprotPdbMappinlLocation());

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chrX", "chrY", "chrM"};

        for (String chr : chromosomes) {

            Dataset<Row> mapToUniprot = SparkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);
            Dataset<Row> mapToPDB = mapToPDBPositions(mapUniprotToPdb, mapToUniprot);
            mapToPDB.write().mode(SaveMode.Overwrite).parquet(pdbmapping+"/"+chr);
        }
    }

    public static void runGencode(String genomeName) throws Exception
    {
        DataLocationProvider.setGenome(genomeName);
        mapToPDBPositions(DataLocationProvider.getGencodeUniprotLocation(),
                DataLocationProvider.getGencodePDBLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToPDBPositions(DataLocationProvider.getExonsUniprotLocation(),
                DataLocationProvider.getExonsPDBLocation());
    }
}
