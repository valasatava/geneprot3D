package org.rcsb.coassociated_exons.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.coassociated_exons.mappers.MapToDomain;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/25/17.
 */
public class GetUniprotAnnotation {

    public static  List<String> run(String chr) throws FileNotFoundException, JAXBException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getExonsUniprotLocation() + "/" + chr);
        JavaRDD<Row> data = mapping
                .drop(mapping.col("isoformIndex"))
                .drop(mapping.col("isoformPosStart"))
                .drop(mapping.col("isoformPosEnd"))
                .toJavaRDD().map(new MapToDomain()).filter(t -> (t != null) );

        List<String> results = data.map( t -> ( t.toString()
                .replace("[","")
                .replace("]","") + "\n") ).collect();
        return results;
    }

    public static void main(String[] args) throws IOException, JAXBException {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                "chr20", "chr21", "chr22", "chrX", "chrY"};

        List<String> results = new ArrayList<String>();

        for (String chr : chromosomes) {

            System.out.println("Prosessing "+chr);
            List<String> data = run(chr);
            results.addAll(data);
            System.out.println("done");
        }

        String filename = "/Users/yana/ishaan/RESULTS/uniprot_domains.csv";
        FileWriter writer = new FileWriter(filename);
        for(String str: results) {
            writer.write(str);
        }
        writer.close();
    }
}
