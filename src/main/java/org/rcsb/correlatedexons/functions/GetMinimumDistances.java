package org.rcsb.correlatedexons.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.mappers.MapToBestStructure;
import org.rcsb.correlatedexons.mappers.MapToMinDistance;
import org.rcsb.correlatedexons.mappers.MapToResolution;
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
public class GetMinimumDistances {

    public static void run(String chr) throws IOException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getExonsStructuralMappingLocation() + "/" + chr);

        JavaRDD<List<String>> data = mapping.toJavaRDD()
                .map(new MapToResolution())
                .filter( t -> t != null )
                .groupBy( t -> t.getString(2) )
                .map(new MapToBestStructure())
                .filter( t -> ( (t != null) && (t.size()>1) ) )
                .map(new MapToMinDistance());

        List<List<String>> results = data.collect();
        List<String> out = new ArrayList<>();
        for (List<String> t : results) {
            for (String l : t ) { out.add(l); }
        }

        String path = "/Users/yana/ishaan/RESULTS/distances/";
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
        for(String str: out) {
            writer.write(str);
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        run("chr21");
    }
}