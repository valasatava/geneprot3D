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

import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

public class HomologyModelsProvider {
	
    /** Read homology models of all human proteins (~20.000) from SwissModel UniProt API into a DataFrame
     *  then write it into file using SwissHomology class.
     *
     * @param
     * @return
     */
    private static void createParquetFileHumanHomologues(String path) throws Exception {

        Dataset<Row> uniprotpdb = MappingDataProvider.getPdbUniprotMapping();
        uniprotpdb.persist().createOrReplaceTempView("humanuniprot");

        // First, get the list of all human uniprot ids
        Dataset<Row> allHumanUniprotIds = SaprkUtils.getSparkSession().sql("select distinct uniProtId from humanuniprot");

        // For each of those uniprot ids, call SWISS uniprot API to get the homology model sequences.
        List<SwissHomology> allUniprotHomologs = new ArrayList<>();

        for (Iterator<Row> it = allHumanUniprotIds.collectAsList().iterator(); it.hasNext(); ) {
            
        	String uniprotId = it.next().get(0).toString();
            JSONArray homologyArray = CommonUtils.readJsonArrayFromUrl("https://swissmodel.expasy.org/repository/uniprot/" + uniprotId + ".json?provider=swissmodel");

            if (homologyArray.length() <= 0)
                continue;

            for (int i = 0; i < homologyArray.length(); i++) {

                SwissHomology swissHomology = new SwissHomology();
                swissHomology.setUniProtId(uniprotId);

                JSONObject homologyObject = homologyArray.getJSONObject(i);
                swissHomology.setModelFromJSONObject(homologyObject);

                allUniprotHomologs.add(swissHomology);

                //Download the structure
                URL url = new URL(swissHomology.getCoordinates());
                File file = new File(DataLocationProvider.getHumanHomologyCoordinatesLocation()+swissHomology.getTemplate()+"_"+swissHomology.getFromPos()+"_"+swissHomology.getToPos());
                FileUtils.copyURLToFile(url, file);

            }
            if (allUniprotHomologs.size() % 1000 == 0) {
            	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                System.out.println( allUniprotHomologs.size() + " " + dateFormat.format(Calendar.getInstance().getTime()));
            }
        }

        Dataset<Row> mydf = SaprkUtils.getSparkSession().createDataFrame(allUniprotHomologs, SwissHomology.class);
        mydf.write().mode(SaveMode.Overwrite).parquet(path);
    }
    
    public static void printSchema() {
    	Dataset<Row> df = SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getHumanHomologyModelsLocation());
    	System.out.println(df.schema());
	}

    public static Dataset<Row> getAllAsDataFrame() {
        return SaprkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getHumanHomologyModelsLocation());
    }

    public static Dataset<Row> get30pcAsDataFrame() {

        Dataset<Row> models = getAllAsDataFrame();

        Dataset<Row> models30pc = models.select("uniProtId", "fromPos", "toPos",
                "similarity", "template", "coordinates", "alignment")
                .filter(models.col("similarity").gt(0.3))
                .withColumnRenamed("fromPos", "fromUniprot")
                .withColumnRenamed("toPos", "toUniprot")
                .drop(models.col("similarity"));

        return models30pc;
    }

    public static void main(String[] args) throws Exception {
        createParquetFileHumanHomologues(DataLocationProvider.getHumanHomologyModelsLocation());
        printSchema();
	}
}
