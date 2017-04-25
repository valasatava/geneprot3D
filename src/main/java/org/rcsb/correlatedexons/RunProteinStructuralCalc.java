package org.rcsb.correlatedexons;

import org.rcsb.correlatedexons.functions.GetMinimumDistances;
import org.rcsb.genevariation.io.DataLocationProvider;

/**
 * Created by yana on 4/17/17.
 */
public class RunProteinStructuralCalc {

    public static void run(String path) throws Exception {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                                "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            System.out.println("Prosessing "+chr);

            GetMinimumDistances.run(chr);

            System.out.println("done");
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        run(DataLocationProvider.getExonsStructuralMappingLocation());
        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }
}