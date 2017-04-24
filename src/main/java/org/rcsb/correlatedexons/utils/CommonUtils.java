package org.rcsb.correlatedexons.utils;

import org.apache.spark.sql.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yana on 4/6/17.
 */
public class CommonUtils {

    public static List<Integer> getIsoStartEndForRow(Row row) {

        int isoformStart;
        int isoformEnd;

        String orientation = row.getString(5);
        if (orientation.equals("+")) {
            isoformStart = row.getInt(0);
            isoformEnd = row.getInt(11);
        }
        else {
            isoformStart = row.getInt(11);
            isoformEnd = row.getInt(0);
        }
        return Arrays.asList(isoformStart, isoformEnd);
    }

    public static void writeListOfStringsInFile( List<String> lst, String path ) throws IOException {
        FileWriter writer = new FileWriter(path);
        for(String str: lst) {
            writer.write(str+"\n");
        }
        writer.close();
    }

    public static List<Row> getPDBStructure(Iterable<Row> data, String pdbId, String chainId) {

        Iterator<Row> it = data.iterator();

        List<Row> bestStruc = new ArrayList<Row>();
        while (it.hasNext()) {
            Row row = it.next();
            if (row.getString(11).equals(pdbId) && row.getString(12).equals(chainId)) {
                bestStruc.add(row);
            }
        }
        return bestStruc;
    }

    public static List<Row> getModelStructure(Iterable<Row> data, String pdbId, String chainId) {

        Iterator<Row> it = data.iterator();

        List<Row> bestStruc = new ArrayList<Row>();
        while (it.hasNext()) {
            Row row = it.next();
            if ( row.getString(15).contains(pdbId) && row.getString(15).contains(chainId) ) {
                bestStruc.add(row);
            }
        }
        return bestStruc;
    }
}
