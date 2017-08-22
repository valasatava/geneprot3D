package org.rcsb.geneprot.common.sandbox;

import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.io.HomologyModelsProvider;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yana on 5/26/17.
 */
public class CheckModelsCoordinates {

    public static void main(String[] args) {

        List<Row> modelsData = HomologyModelsProvider.getAsDataFrame(DataLocationProvider.getHomologyModelsLocation()).collectAsList();
        List<String> templates = modelsData.stream().map(t -> t.getString(12) + "_" + t.getInt(3) + "_" + t.getInt(13)).collect(Collectors.toList());
        for (String t : templates) {
            File file = new File(DataLocationProvider.getHomologyModelsCoordinatesLocation() + t + ".pdb");
            if (!file.exists()) {
                System.out.println(file.getAbsolutePath());
            }
        }

    }
}
