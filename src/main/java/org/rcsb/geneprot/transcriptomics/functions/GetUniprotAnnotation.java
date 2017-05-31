package org.rcsb.geneprot.transcriptomics.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.transcriptomics.mapfunctions.MapToUniprotFeature;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;

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

    public static  List<String> runForChromosome(String chr, Function<Row, Row> f) throws FileNotFoundException, JAXBException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getGencodeUniprotLocation() + "/" + chr);
        JavaRDD<Row> data = mapping
                .drop(mapping.col("isoformIndex"))
                .drop(mapping.col("isoformPosStart"))
                .drop(mapping.col("isoformPosEnd"))
                .toJavaRDD().map(f).filter(t -> (t != null) );

        List<String> results = data.map( t -> ( t.toString()
                .replace("[","")
                .replace("]","") + "\n") ).collect();
        return results;
    }

    public static List<String> run(Function<Row, Row> f) throws IOException, JAXBException {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                "chr20", "chr21", "chr22", "chrX", "chrY"};

        List<String> results = new ArrayList<String>();

        for (String chr : chromosomes) {

            System.out.println("Prosessing "+chr);
            List<String> data = runForChromosome(chr, f);
            results.addAll(data);
            System.out.println("done");
        }

        return results;
    }

    public static void main(String[] args) throws IOException, JAXBException {

//        MapToUniprotFeature f = new MapToUniprotFeature("domain");
//        String filename = DataLocationProvider.getExonsProjectResults()+"gencode.v24.CDS.uniprot_domains.csv";

//        MapToUniprotFeature f = new MapToUniprotFeature("active site");
//        String filename = DataLocationProvider.getExonsProjectResults()+"gencode.v24.CDS.uniprot_active_sites.csv";

        MapToUniprotFeature f1 = new MapToUniprotFeature("topological domain");
        MapToUniprotFeature f2 = new MapToUniprotFeature("transmembrane region");
        String filename = DataLocationProvider.getExonsProjectResults()+"gencode.v24.CDS.topological_domains.csv";

        List<String> results1 = run(f1);
        List<String> results2 = run(f2);

        List<String> results = new ArrayList<>();
        results.addAll(results1);
        results.addAll(results2);
        FileWriter writer = new FileWriter(filename);
        for(String str: results) {
            writer.write(str);
        }
        writer.close();
    }
}
