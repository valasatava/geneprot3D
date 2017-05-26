package org.rcsb.geneprot.transcriptomics.analysis;

import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.transcriptomics.datastructures.ExonBoundariesPair;
import org.rcsb.geneprot.transcriptomics.io.CustomDataProvider;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by yana on 5/25/17.
 */
public class NGLScriptsGeneration {

    public static void run() throws IOException {

        List<ExonBoundariesPair> pairs = CustomDataProvider.getCoordinatedPairs(Paths.get(DataLocationProvider.getExonsProjectData()
                + "/coordinated_pairs.csv"));

    }

    public static void main(String[] args) {
        try {
            run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}