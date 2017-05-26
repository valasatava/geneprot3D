package org.rcsb.geneprot.transcriptomics.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.transcriptomics.functions.GetDistances;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SaprkUtils;

import java.io.FileWriter;
import java.util.List;

/**
 * Created by yana on 4/17/17.
 */
public class RunProteinStructuralCalc {

    public static void run(String path) throws Exception {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                                "chr20", "chr21", "chr22", "chrX", "chrY"};

//        String[] chromosomes = {"chr1"};

        for (String chr : chromosomes) {

            System.out.println("Prosessing "+chr);

            Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(path + "/" + chr);
            List<String> results = GetDistances.run(mapping);

            String filename = DataLocationProvider.getExonsProjectResults()+"distances/"+chr+"_gencode.v24.CDS.protein_coding.exons_distances.csv";
            FileWriter writer = new FileWriter(filename);
            for(String str: results) {
                writer.write(str);
            }
            writer.close();

            System.out.println("done");
        }

    }

    public static void main(String[] args) throws Exception {

        long start = System.nanoTime();

//        run(DataLocationProvider.getExonsStructuralMappingLocation());
        run(DataLocationProvider.getGencodeStructuralMappingLocation());

        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }
}