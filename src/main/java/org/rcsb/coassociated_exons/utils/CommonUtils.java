package org.rcsb.coassociated_exons.utils;

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
        } else {
            isoformStart = row.getInt(11);
            isoformEnd = row.getInt(0);
        }
        return Arrays.asList(isoformStart, isoformEnd);
    }

    public static void writeListOfStringsInFile(List<String> lst, String path) throws IOException {
        FileWriter writer = new FileWriter(path);
        for (String str : lst) {
            writer.write(str + "\n");
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
        List<Row> best = new ArrayList<Row>();
        while (it.hasNext()) {
            Row row = it.next();
            if (row.getString(15).contains(pdbId) && row.getString(15).contains(chainId)) {
                best.add(row);
            }
        }
        return best;
    }

    public static String[] getStructureWithBestResolution(Iterable<Row> data, List<String> keys) {

        String[] best = new String[2];
        if ( keys.size() == 1 ) {
            best[0] = keys.get(0).split("_")[0];
            best[1] = keys.get(0).split("_")[1];
        }
        else {
            float bestRes = 99.9f;
            Iterator<Row> it = data.iterator();
            for (String key : keys) {
                String pdbIdb = key.split("_")[0];
                String chainId = key.split("_")[1];
                while (it.hasNext()) {
                    Row row = it.next();
                    String pdbIdRow = RowUtils.getPdbId(row);
                    String chainIdRow = RowUtils.getChainId(row);
                    if (pdbIdb.equals(pdbIdRow) && chainId.equals(chainIdRow)) {
                        float resolution = RowUtils.getResolution(row);
                        if (resolution < bestRes) {
                            best[0] = pdbIdRow;
                            best[1] = chainIdRow;
                            bestRes = resolution;
                        }
                    }
                }
            }
        }
        return best;
    }
}

