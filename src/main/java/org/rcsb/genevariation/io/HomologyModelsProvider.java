package org.rcsb.genevariation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.genevariation.datastructures.SwissHomology;
import org.rcsb.genevariation.utils.CommonUtils;
import org.rcsb.genevariation.utils.SaprkUtils;

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
        for (Iterator<Row> iter = allHumanUniprotIds.collectAsList().iterator(); iter.hasNext(); ) {
            
        	String uniprotId = iter.next().get(0).toString();
            JSONArray homologyArray = CommonUtils.readJsonArrayFromUrl("https://swissmodel.expasy.org/repository/uniprot/" + uniprotId + ".json?provider=swissmodel");

            if (homologyArray.length() <= 0)
                continue;

            for (int i = 0; i < homologyArray.length(); i++) {
                JSONObject homologyObject = homologyArray.getJSONObject(i);

                SwissHomology swissHomology = new SwissHomology();
                swissHomology.setUniProtId(uniprotId);

                swissHomology.setFromPos(homologyObject.getInt("from"));
                swissHomology.setToPos(homologyObject.getInt("to"));
                swissHomology.setAlignment(homologyObject.getString("alignment"));
                swissHomology.setCoordinates(homologyObject.getString("coordinates"));

                swissHomology.setCrc64(homologyObject.has("crc64") ? homologyObject.getString("crc64") : null);
                swissHomology.setGmqe(homologyObject.has("gmqe") ? homologyObject.getDouble("gmqe") : null);
                swissHomology.setIdentity(homologyObject.has("identity") ? homologyObject.getDouble("identity") : null);
                swissHomology.setMd5(homologyObject.has("md5") ? homologyObject.getString("md5") : null);
                swissHomology.setMethod(homologyObject.has("method") ? homologyObject.getString("method") : null);
                swissHomology.setOligo_state(homologyObject.has("oligo-state") ? homologyObject.getString("oligo-state") : null);
                swissHomology.setProvider(homologyObject.has("provider") ? homologyObject.getString("provider") : null);
                swissHomology.setQmean(homologyObject.has("qmean") ? homologyObject.getDouble("qmean") : null);
                swissHomology.setSimilarity(homologyObject.has("similarity") ? homologyObject.getDouble("similarity") : null);
                swissHomology.setTemplate(homologyObject.has("template") ? homologyObject.getString("template") : null);

                allUniprotHomologs.add(swissHomology);
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
        return SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getHumanHomologyModelsLocation());
    }

    public static Dataset<Row> get30pcAsDataFrame() {
        return SaprkUtils.getSparkSession().read().parquet(DataLocationProvider.getHomologyModelsMappingLocation());
    }

    public static void main(String[] args) throws Exception {
        createParquetFileHumanHomologues(DataLocationProvider.getHumanHomologyModelsLocation());
    	printSchema();
	}
}
