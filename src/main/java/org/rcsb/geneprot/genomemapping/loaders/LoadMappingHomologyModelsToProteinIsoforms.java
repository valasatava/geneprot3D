package org.rcsb.geneprot.genomemapping.loaders;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.sql.SparkSession;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** This loader maps isoforms to UniProt isoform sequences.
 *
 * Created by Yana Valasatava on 11/7/17.
 */
public class LoadMappingHomologyModelsToProteinIsoforms extends AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(LoadMappingHomologyModelsToProteinIsoforms.class);

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static void main(String[] args) throws Exception {

        logger.info("Started loading genome to uniprot mapping...");
        long timeS = System.currentTimeMillis();

        setArguments(args);

        // TODO

        long timeE = System.currentTimeMillis();
        logger.info("Completed. Time taken: " + DurationFormatUtils.formatPeriod(timeS, timeE, "HH:mm:ss:SS"));
    }
}