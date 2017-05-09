package org.rcsb.exonscoassociation.filters;

import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.CommonUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 5/9/17.
 */
public class FilterExonsPairs {

    public static void run() throws IOException {

        //
        String results_path = DataLocationProvider.getExonsProjectResults()
                +"gencode.v24.CDS.protein_coding.exons_distances.csv";
        List<String> results = Files.readAllLines(Paths.get(results_path));

        // pairs of exons
        String file_path = DataLocationProvider.getExonsProjectData()
                +"coordinated_pairs.csv";
        List<String> data = Files.readAllLines(Paths.get(file_path));

        List<String> filtered_data = new ArrayList<>();
        for (String line : data) {

            String exon1 = line.split(",")[0].split("_")[1]
                    +"_"+line.split(",")[0].split("_")[2];
            String exon2 = line.split(",")[1].split("_")[1]
                    +"_"+line.split(",")[1].split("_")[2];

            for ( String rline : results ) {
                if ( rline.contains(exon2) && rline.contains(exon1) ) {
                    filtered_data.add(rline);
                }
            }
        }
        String out_path = DataLocationProvider.getExonsProjectResults()
                +"exon_pairs.distances.csv";
        CommonUtils.writeListToFile(filtered_data, out_path);
    }

    public static void main(String[] args) throws IOException {
        run();
    }
}
