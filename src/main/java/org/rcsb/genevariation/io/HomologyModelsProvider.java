package org.rcsb.genevariation.io;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.genevariation.datastructures.SwissHomology;
import org.rcsb.genevariation.utils.CommonUtils;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class HomologyModelsProvider {
	
    /** Read homology models of all human proteins (~20.000) from SwissModel UniProt API into a DataFrame
     *  then write it into file using SwissHomology class.
     */

    private static final Logger logger = LoggerFactory.getLogger(HomologyModelsProvider.class);

    public static List<SwissHomology> getModelsForUniProtIds(List<String> uniProtIds) throws Exception {

        List<SwissHomology> models = new ArrayList<>();

        for (String uniProtId : uniProtIds) {

            JSONArray homologyArray = null;
            try {
                // Returns a JSON formatted file with a list of homology models for UniProtKB entry
                homologyArray = CommonUtils.readJsonArrayFromUrl("https://swissmodel.expasy.org/repository/uniprot/"
                        + uniProtId + ".json?provider=swissmodel"); }

            catch ( FileNotFoundException fnfe ) {
                logger.warn("SWISS-MODEL doesn't have "+uniProtId+" UniProt entry");
                continue; }

            if (homologyArray.length() <= 0) {
                logger.info(uniProtId + " has no homology models");
                continue;
            }

            for (int i = 0; i < homologyArray.length(); i++) {

                SwissHomology swissHomology = new SwissHomology();
                swissHomology.setUniProtId(uniProtId);

                JSONObject homologyObject = homologyArray.getJSONObject(i);
                swissHomology.setModelFromJSONObject(homologyObject);

                models.add(swissHomology);
                if (models.size() % 1000 == 0) {
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    logger.info( models.size() + " " + dateFormat.format(Calendar.getInstance().getTime()));
                }
            }
        }
        return models;
    }

    /** Creates a parquet file with the following schema
     *+----------+-------------+-------+-------+------+----------+-----+--------+-------------+----------+-------+------------+----------+-------+----------+
     | alignment | coordinates | crc64 |fromPos| gmqe | identity | md5 | method | oligo_state | provider | qmean | similarity | template | toPos | uniProtId |
      +----------+-------------+-------+-------+------+----------+-----+--------+-------------+----------+-------+------------+----------+-------+----------+
     *
     * @param homologyModels list of SwissHomology objects
     * @param path a path to write out a parquet file
     */
    public static void createParquetFile(List<SwissHomology> homologyModels, String path) {

        Dataset<Row> mydf = SaprkUtils.getSparkSession()
                .createDataFrame(homologyModels, SwissHomology.class);
        mydf.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public static void downloadCoordinates(List<SwissHomology> homologyModels, String path) throws IOException {

        for (SwissHomology swissHomology : homologyModels) {

            URL url = new URL(swissHomology.getCoordinates());
            File file = new File(path + swissHomology.getTemplate() + "_" +
                    swissHomology.getFromPos() + "_" +
                    swissHomology.getToPos()+".pdb");

            //Download the structure if file doesn't exist
            if (!file.exists())
                FileUtils.copyURLToFile(url, file);
        }

    }

    public static Dataset<Row> getAsDataFrame(String path) {
        return SaprkUtils.getSparkSession().read()
                .parquet(path);
    }

    public static Dataset<Row> getAsDataFrame30pc(String path) {

        Dataset<Row> models = getAsDataFrame(path);

        Dataset<Row> models30pc = models.select("uniProtId", "fromPos", "toPos",
                "similarity", "template", "coordinates", "alignment")
                .filter(models.col("similarity").gt(0.3))
                .withColumnRenamed("fromPos", "fromUniprot")
                .withColumnRenamed("toPos", "toUniprot")
                .drop(models.col("similarity"));

        return models30pc;
    }

    public static void main(String[] args) throws Exception {

	}
}
