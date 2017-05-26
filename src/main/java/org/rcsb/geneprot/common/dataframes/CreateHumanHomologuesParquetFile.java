package org.rcsb.geneprot.common.dataframes;

import org.rcsb.geneprot.common.io.HomologyModelsProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.rcsb.geneprot.common.datastructures.SwissHomology;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;

/** Read homology models of all human proteins from SwissModel into a DataFrame
 *  then write it into a parquet file using SwissHomology class.
 *
 * Created by Yana Valasatava on 5/23/17.
 */
public class CreateHumanHomologuesParquetFile {

    private static final Logger logger = LoggerFactory.getLogger(CreateHumanHomologuesParquetFile.class);

    public static List<SwissHomology> getModelsFromSMR() throws Exception {

        logger.info("Getting the UniProt accessory codes for human proteins...");
        String referenceProteome = "9606";
        File file = new File(DataLocationProvider.getHumanModelsJSONFileLocation());
        HomologyModelsProvider.downloadJSONFileForReferenceProteome(referenceProteome, file);
        List<String> uniprotIds = HomologyModelsProvider.getUniprotIdsFromJSONFile(file);
        logger.info("...done.");

        logger.info("Retriving the homolody models from SWISS-MODEL repository...");
        List<SwissHomology> models = HomologyModelsProvider.getModelsFromSMR(uniprotIds);
        logger.info("...done.");

        return models;
    }

    public static List<SwissHomology> getModelsFromLocalFile() throws Exception {

        logger.info("Retriving the homolody models from JSON file...");
        String referenceProteome = "9606";
        File file = new File(DataLocationProvider.getHumanModelsJSONFileLocation());
        HomologyModelsProvider.downloadJSONFileForReferenceProteome(referenceProteome, file);
        List<SwissHomology> models = HomologyModelsProvider.getModelsFromJSONFile(file);
        logger.info("...done.");

        return models;
    }

    public static void run() throws Exception {

        List<SwissHomology> models = getModelsFromSMR();

        logger.info("Creating the parquet file...");
        HomologyModelsProvider.createParquetFile(models, DataLocationProvider.getHumanHomologyModelsLocation());
        logger.trace("...done.");

        logger.info("Downloading the structure of models (if missing)");
        HomologyModelsProvider.downloadCoordinates(models, DataLocationProvider.getHumanHomologyCoordinatesLocation());
        logger.info("...the task is executed successfully!");
    }

    public static void main(String args[]) {

        try { run(); }

        catch ( MalformedURLException mfe ) {
            logger.error("Formatting problem with SWISS-MODEL homology models API URL"); }

        catch (Exception e) {
            logger.error("Error: something went wrong: " + e.getMessage()); }

        finally { SaprkUtils.stopSparkSession(); }
    }
}