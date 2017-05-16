package org.rcsb.exonscoassociation.utils;

import org.apache.spark.sql.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.Math.abs;

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

    public static List<Row> getPDBStructure(Iterable<Row> data, String[] key) {

        String pdbId = key[0];
        String chainId = key[1];

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

    public static List<Row> getModelStructure(Iterable<Row> data, String[] key) {

        String pdbId = key[0];
        String chainId = key[1];

        int start = Integer.valueOf(key[2]);
        int end = Integer.valueOf(key[3]);

        Iterator<Row> it = data.iterator();
        List<Row> best = new ArrayList<Row>();

        while (it.hasNext()) {
            Row row = it.next();
            if (row.getString(15).contains(pdbId) && row.getString(15).contains(chainId)
                    && row.getInt(13)==start && row.getInt(14)==end) {
                best.add(row);
            }
        }
        return best;
    }

    public static String[] getStructureWithBestResolution(Iterable<Row> data, List<String> keys) throws Exception {

        String[] best = new String[4];

        if ( keys.size() == 1 ) {
            best[0] = keys.get(0).split("_")[0];
            best[1] = keys.get(0).split("_")[1];
            best[2] = keys.get(0).split("_")[3];
            best[3] = keys.get(0).split("_")[4];
        }
        else {

            int bestStart = -1;
            int bestEnd = -1;
            float bestRes = 99.9f;

            Iterator<Row> it = data.iterator();

            for (String key : keys) {

                String pdbIdb = key.split("_")[0];
                String chainId = key.split("_")[1];
                int start = Integer.valueOf(key.split("_")[3]);
                int end = Integer.valueOf(key.split("_")[4]);

                while (it.hasNext()) {

                    Row row = it.next();

                    String pdbIdRow = RowUtils.getPdbId(row);
                    String chainIdRow = RowUtils.getChainId(row);

                    int startRow = RowUtils.getStructureStart(row);
                    int endRow = RowUtils.getStructureEnd(row);

                    if ( pdbIdb.equals(pdbIdRow) && chainId.equals(chainIdRow) && start==startRow && end==endRow ) {

                        float resolution = RowUtils.getResolution(row);

                        if (resolution < bestRes) {

                            best[0] = pdbIdRow;
                            best[1] = chainIdRow;
                            best[2] = String.valueOf(startRow);
                            best[3] = String.valueOf(endRow);

                            bestRes = resolution;
                            bestStart = startRow;
                            bestEnd = endRow;
                        }
                        else if (resolution == bestRes) {

                            if ( (bestStart != -1 && start != -1) && (bestEnd != -1 && end != -1) ) {

                                if ( abs(end-start) > abs(bestEnd-bestStart) ) {

                                    best[0] = pdbIdRow;
                                    best[1] = chainIdRow;
                                    best[2] = String.valueOf(bestStart);
                                    best[3] = String.valueOf(bestEnd);

                                    bestStart = start;
                                    bestEnd = end;

                                }
                            }
                        }
                    }
                }
            }
        }
        return best;
    }

    public static int getResponseCode(String urlString) throws MalformedURLException, IOException {
        URL u = new URL(urlString);
        HttpURLConnection huc =  (HttpURLConnection)  u.openConnection();
        huc.setRequestMethod("GET");
        huc.connect();
        return huc.getResponseCode();
    }
}

