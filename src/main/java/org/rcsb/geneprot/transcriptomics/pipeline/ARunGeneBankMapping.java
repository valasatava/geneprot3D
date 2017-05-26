package org.rcsb.geneprot.transcriptomics.pipeline;

import org.rcsb.geneprot.genes.datastructures.ExonSerializable;
import org.rcsb.geneprot.common.io.MappingDataProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.rcsb.geneprot.transcriptomics.utils.ExonsUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import java.io.IOException;
import java.util.List;

/**
 * Created by yana on 4/13/17.
 */
public class ARunGeneBankMapping {

    public static void mapToGeneBank(List<ExonSerializable> exons, String path) throws IOException {

        Dataset<Row> exonsData = SaprkUtils.getSparkSession().createDataFrame(exons, ExonSerializable.class);

        Dataset<Row> geneBankToEnsembleMapping = MappingDataProvider.getGeneBankToEnsembleMapping();

        Dataset<Row> mapping = exonsData.join(geneBankToEnsembleMapping,
                exonsData.col("ensemblId").equalTo(geneBankToEnsembleMapping.col("ensemblId")))
                .select(exonsData.col("chromosome"), exonsData.col("geneName"),
                        exonsData.col("ensemblId"), geneBankToEnsembleMapping.col("geneBankId"),
                        exonsData.col("orientation"), exonsData.col("offset"),
                        exonsData.col("start"), exonsData.col("end"));

        mapping.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public static void mapToGeneBank(String datapath, String mappingpath) throws Exception {

        List<ExonSerializable> exons = ExonsUtils.getSerializableExons(datapath);
        mapToGeneBank(exons, mappingpath);
    }

    public static void runGencodeV24() throws Exception {
        mapToGeneBank(DataLocationProvider.getGencodeProteinCodingDataLocation(),
                DataLocationProvider.getGencodeGeneBankLocation());
    }

    public static void runCorrelatedExons() throws Exception {
        mapToGeneBank(DataLocationProvider.getExonsProteinCodingDataLocation(),
                DataLocationProvider.getExonsGeneBankLocation());
    }
}