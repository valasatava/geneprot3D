package org.rcsb.genevariation.mappers;

/**
 * Created by yana on 4/21/17.
 */
public class UniprotToHomologyModelMapper {

    private static int[] uniprotCoordinates;
    private static int[] modelCoordinates;

    public static void map(String[] alignment) {

        uniprotCoordinates = new int[alignment.length];
        modelCoordinates = new int[alignment.length];

    }
}
