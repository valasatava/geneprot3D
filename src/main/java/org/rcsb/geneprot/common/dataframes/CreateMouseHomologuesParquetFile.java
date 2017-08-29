package org.rcsb.geneprot.common.dataframes;

import org.rcsb.geneprot.common.datastructures.SwissHomology;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.io.HomologyModelsProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;

/** Read homology models of all mouse proteins from SwissModel into a DataFrame
 *  then write it into a parquet file using SwissHomology class.
 *
 * Created by Yana Valasatava on 5/23/17.
 */
public class CreateMouseHomologuesParquetFile {

    private static final Logger logger = LoggerFactory.getLogger(CreateMouseHomologuesParquetFile.class);

    public static void run() throws Exception {

        logger.info("Getting the UniProt accessory codes for human proteins...");

        // Mus musculus
        String taxonomyId = "10090";
        DataLocationProvider.setGenome("mouse");
        logger.info("Processing genome: " + DataLocationProvider.getGenome());

        File file = new File(DataLocationProvider.getHomologyModelsJSONFileLocation());
        if (! file.exists())
            HomologyModelsProvider.downloadJSONFileForReferenceProteome(taxonomyId, file);
        List<String> uniprotIds = HomologyModelsProvider.getUniprotIdsFromJSONFile(file);
        logger.info("...done.");

        logger.info("Retrieving the homology models from SWISS-MODEL repository to: " + DataLocationProvider.getHomologyModelsCoordinatesLocation());
        List<SwissHomology> models = HomologyModelsProvider.getModelsFromSMR(uniprotIds, DataLocationProvider.getHomologyModelsCoordinatesLocation());
        logger.info("...done.");

        logger.info("Creating the parquet file: " + DataLocationProvider.getHomologyModelsLocation());
        HomologyModelsProvider.createParquetFile(models, DataLocationProvider.getHomologyModelsLocation());
        logger.info("Parquet file with homology models is created.");
    }

    public static void main(String args[]) throws Exception {

        try { run(); }

        catch ( MalformedURLException mfe ) {
            logger.error("Formatting problem with SWISS-MODEL homology models API URL: "+ mfe.getMessage()); }

        finally { SparkUtils.stopSparkSession(); }
    }
}