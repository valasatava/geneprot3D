package org.rcsb.geneprot.common.io;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.biojava.nbio.core.util.FileDownloadUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.rcsb.geneprot.common.datastructures.SwissHomology;
import org.rcsb.geneprot.common.utils.CommonUtils;
import org.rcsb.geneprot.common.utils.FileCustomUtils;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HomologyModelsProvider {

    private static final Logger logger = LoggerFactory.getLogger(HomologyModelsProvider.class);

    public static List<SwissHomology> parseJSONArrayToSwissHomology(JSONArray array) {

        List<SwissHomology> models = new ArrayList<>();

        for (int i = 0; i < array.length(); i++) {

            JSONObject obj = array.getJSONObject(i);

            SwissHomology swissHomology = new SwissHomology();

            swissHomology.setUniProtId(obj.getString("uniprot_ac"));
            swissHomology.setFromPos(obj.getInt("from"));
            swissHomology.setToPos(obj.getInt("to"));

            swissHomology.setProvider(obj.has("provider") ? obj.getString("provider") : null);
            swissHomology.setQmean(obj.has("qmean") ? obj.getDouble("qmean") : null);
            swissHomology.setSimilarity(obj.has("similarity") ? obj.getDouble("similarity") : null);
            swissHomology.setTemplate(obj.has("template") ? obj.getString("template") : null);

            models.add(swissHomology);
        }
        return models;
    }

    public static List<SwissHomology> parseJSONArrayToSwissHomology(SwissHomology swissHomology, JSONArray array) {

        List<SwissHomology> models = new ArrayList<>();

        for (int i = 0; i < array.length(); i++) {

            JSONObject obj = array.getJSONObject(i);

            swissHomology.setFromPos(obj.getInt("from"));
            swissHomology.setToPos(obj.getInt("to"));
            swissHomology.setAlignment(obj.getString("alignment"));
            swissHomology.setCoordinates(obj.getString("coordinates"));

            swissHomology.setCrc64(obj.has("crc64") ? obj.getString("crc64") : null);
            swissHomology.setGmqe(obj.has("gmqe") ? obj.getDouble("gmqe") : null);
            swissHomology.setIdentity(obj.has("identity") ? obj.getDouble("identity") : null);
            swissHomology.setMd5(obj.has("md5") ? obj.getString("md5") : null);
            swissHomology.setMethod(obj.has("method") ? obj.getString("method") : null);
            swissHomology.setOligo_state(obj.has("oligo-state") ? obj.getString("oligo-state") : null);
            swissHomology.setProvider(obj.has("provider") ? obj.getString("provider") : null);
            swissHomology.setQmean(obj.has("qmean") ? obj.getDouble("qmean") : null);
            swissHomology.setSimilarity(obj.has("similarity") ? obj.getDouble("similarity") : null);
            swissHomology.setTemplate(obj.has("template") ? obj.getString("template") : null);

            models.add(swissHomology);
        }
        return models;
    }

    public static List<SwissHomology> getModelsFromJSONFile(File file) throws Exception {

        JSONArray homologyArray = FileCustomUtils.readJsonArrayFromLocalFile(file);
        List<SwissHomology> models = parseJSONArrayToSwissHomology(homologyArray);

        return models;
    }

    public static List<String> getUniprotIdsFromJSONFile(File file) throws Exception {

        List<String> uniprotIds = new ArrayList<>();
        JSONArray array = FileCustomUtils.readJsonArrayFromLocalFile(file);

        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);
            // ignore experimental structures
            if (obj.getString("provider").equals("pdb"))
                continue;
            uniprotIds.add(obj.getString("uniprot_ac"));
        }
        return uniprotIds;
    }

    public static List<SwissHomology> getModelsFromSMR(List<String> uniProtIds) throws Exception {

        logger.info( "Requesting data for a total number of UniProt sequences: " + uniProtIds.size() );
        List<SwissHomology> models = new ArrayList<>();

        for (String uniProtId : uniProtIds) {

            JSONArray homologyArray = null;
            try { homologyArray = CommonUtils.readJsonArrayFromUrl("https://swissmodel.expasy.org/repository/uniprot/"
                        + uniProtId + ".json?provider=swissmodel"); }

            catch ( FileNotFoundException fnfe ) {
                logger.warn("SWISS-MODEL doesn't have "+uniProtId+" UniProt entry");
                continue; }

            catch ( IOException ioe ) {
                logger.error("IOException for "+uniProtId+" UniProt entry");
                continue; }

            if (homologyArray.length() <= 0) {
                logger.info(uniProtId + " has no homology models");
                continue;
            }

            SwissHomology swissHomology = new SwissHomology();
            swissHomology.setUniProtId(uniProtId);
            models.add(parseJSONArrayToSwissHomology(swissHomology, homologyArray).get(0));
            if (models.size() % 1000 == 0) {
                logger.info( "accumulated data for " + models.size() + " models" );
            }
        }
        return models;
    }

    /** Creates a parquet file with the following schema
     *+----------+-------------+-------+-------+------+----------+-----+--------+-------------+----------+-------+------------+----------+-------+----------+
     | alignment | coordinates | crc64 |fromPos| gmqe | identity | md5 | method | oligo_state | provider | qmean | similarity | template | toPos | uniProtId |
      +----------+-------------+-------+-------+------+----------+-----+--------+-------------+----------+-------+------------+----------+-------+----------+
     *
     * @param models list of SwissHomology objects
     * @param path a path to write out a parquet file
     */
    public static void createParquetFile(List<SwissHomology> models, String path) {

        Dataset<Row> mydf = SaprkUtils.getSparkSession()
                .createDataFrame(models, SwissHomology.class);
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

    public static void downloadJSONFileForReferenceProteome(String referenceProteome, File fileLocalLocation) throws IOException {

        String url = "https://swissmodel.expasy.org/repository/download/core_species/"+referenceProteome+"_meta.tar.gz";
        File tmp = File.createTempFile(referenceProteome,"_meta.tar.gz");
        logger.info("Downloading " + tmp.getName() + " to " + tmp.getAbsolutePath());
        FileDownloadUtils.downloadFile(new URL(url), tmp);

        logger.info("Extracting "+ tmp.getAbsolutePath() + "content to " + tmp.getParent());
        File destination = new File(tmp.getParent());
        Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");
        archiver.extract(tmp, destination);

        logger.info("Renaming " + tmp.getAbsolutePath() +" to " + fileLocalLocation.getAbsolutePath());
        Path p = Paths.get(fileLocalLocation.getAbsolutePath());
        Path dir = p.getParent();
        if (! Files.exists(dir)) { Files.createDirectories(dir); }
        File target = new File(tmp.getParent() + "/SWISS-MODEL_Repository/INDEX_JSON");
        target.renameTo(fileLocalLocation);

        logger.info("Cleaning up the temporary directory: "+ tmp.getParent());
        tmp.delete();
        File tmpDir = new File(tmp.getParent() + "/SWISS-MODEL_Repository");
        FileUtils.deleteDirectory(tmpDir);
    }
}