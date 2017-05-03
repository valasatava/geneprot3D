package org.rcsb.exonscoassociation.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.exonscoassociation.mappers.MapToBestStructure;
import org.rcsb.exonscoassociation.mappers.MapToDistances;
import org.rcsb.exonscoassociation.mappers.MapToResolution;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/20/17.
 */
public class GetDistances {

    public static List<String> run(Dataset<Row> mapping) {

        JavaRDD<List<String>> data = mapping.toJavaRDD()
                .filter( t -> ( t.getInt(9) != -1 && t.getInt(10) != -1 ))
                //.filter( t -> t.getString(2).equals("ENST00000430393"))
                .map(new MapToResolution())
                .filter( t -> t != null )
                .groupBy( t -> t.getString(2) )
                .map(new MapToBestStructure())
                .filter( t -> ( (t != null) && (t.size()>1) ) )
                .map(new MapToDistances());

        List<List<String>> results = data.collect();
        List<String> out = new ArrayList<>();
        for (List<String> t : results) {
            for (String l : t ) { out.add(l); }
        }
        return out;
    }

    public static void runExons(String chr) throws IOException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getExonsStructuralMappingLocation() + "/" + chr);

        List<String> results = run(mapping);

        String path = DataLocationProvider.getExonsProjectResults()+"/distances/";
        File file = new File(path+chr);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory is created for "+chr);
            } else {
                System.out.println("Directory for "+chr+" already exists!");
            }
        }

        String filename = path+chr+"/minimum_distances.csv";
        FileWriter writer = new FileWriter(filename);
        for(String str: results) {
            writer.write(str);
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        runExons("chr21");
    }
}