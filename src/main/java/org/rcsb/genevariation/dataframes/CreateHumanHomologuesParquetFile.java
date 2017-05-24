package org.rcsb.genevariation.dataframes;

import org.rcsb.genevariation.datastructures.SwissHomology;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.HomologyModelsProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/** Creates a parquet file with homology models for human proteins that can be mapped to UniProt
 *
 * Created by Yana Valasatava on 5/23/17.
 */
public class CreateHumanHomologuesParquetFile {

    private static final Logger logger = LoggerFactory.getLogger(CreateHumanHomologuesParquetFile.class);

    public static void run() throws Exception {

        logger.info("Getting the UniProt accessory codes for human proteins...");
        List<String> uniprotIds = MappingDataProvider.getPdbUniprotMapping()
                .select(col("uniProtId")).distinct()
                .toJavaRDD().map(t -> t.getString(0))
                .collect();
        logger.info("...done.");

        logger.info("Retriving the homolody models from SWISS-MODEL repository...");
        List<SwissHomology> models = HomologyModelsProvider.getModelsForUniProtIds(uniprotIds);
        logger.info("...done.");

        logger.info("Creating the parquet file...");
        HomologyModelsProvider.createParquetFile(models, DataLocationProvider.getHumanHomologyModelsLocation());
        logger.trace("...done.");

        logger.info("Downloading the structure of models (if missing)");
        HomologyModelsProvider.downloadCoordinates(models, DataLocationProvider.getHumanHomologyCoordinatesLocation());
        logger.info("...the task is executed successfully!");
    }

    public static void main(String args[]) throws Exception {

        try { run(); }

        catch ( MalformedURLException mfe ) {
            logger.error("Formatting problem with SWISS-MODEL homology models API URL"); }

        catch (Exception e) {
            logger.error("Error: something went wrong...!"); }

        finally { SaprkUtils.stopSparkSession(); }
    }
}