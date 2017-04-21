package org.rcsb.correlatedexons.utils;

import org.apache.spark.sql.Row;

import java.util.regex.Pattern;

/**
 * Created by yana on 4/20/17.
 */
public class RowUtils {

    public static String getPdbId(Row row) {

        String pdbId = row.getString(11);
        if (pdbId.equals("null")) {
            pdbId = row.getString(15).split(Pattern.quote("."))[0];
        }
        return pdbId;
    }

    public static String getChainId(Row row) {

        String chainId = row.getString(12);
        if (chainId.equals("null")) {
            chainId = row.getString(15).split(Pattern.quote("."))[2];
        }
        return chainId;
    }

    public static String getExon(Row row) {
        return String.valueOf(row.getInt(4)) + "_" +
                String.valueOf(row.getInt(5)) + "_" +
                String.valueOf(row.getInt(7));
    }

    public static boolean isPDBStructure(Row row) {
        if ( row.getString(11).equals("null") )
            return false;
        return true;
    }

    public static boolean isForward(Row row) {
        if (row.getString(6).equals("+"))
            return true;
        return false;
    }

    public static String getModelCoordinates(Row row) {
        return row.getString(16);
    }

    public static int getUniProtStart(Row row) {

        if ( isForward(row) ) {
            if (row.get(9) == null) {
                return -1;
            }
            else {
                return row.getInt(9);
            }
        }
        else {
            if (row.get(10) == null) {
                return -1;
            }
            else {
                return row.getInt(10);
            }
        }
    }

    public static int getUniProtEnd(Row row) {

        if ( isForward(row) ) {
            if (row.get(10) == null) {
                return -1;
            }
            else {
                return row.getInt(10);
            }
        }
        else {
            if (row.get(9) == null) {
                return -1;
            }
            else {
                return row.getInt(9);
            }
        }
    }

    public static int getPdbStart(Row row) {

        if ( ! isPDBStructure(row)) {

            if ( isForward(row) ) {

                if ( row.getInt(13) <= row.getInt(9) ) {
                    return row.getInt(9);
                }
                else {
                    return row.getInt(13);
                }
            }
            else {
                if ( row.getInt(13) <= row.getInt(10) ) {
                    return row.getInt(10);
                }
                else {
                    return row.getInt(13);
                }
            }
        }
        else {
            if ( isForward(row) ) {
                if ( row.get(13)==null )
                    return -1;
                return row.getInt(13);
            }
            else {
                if ( row.get(14)==null )
                    return -1;
                return row.getInt(14);
            }
        }
    }

    public static int getPdbEnd(Row row) {

        if ( ! isPDBStructure(row)) {

            if ( isForward(row) ) {

                if ( row.getInt(14) <= row.getInt(10) ) {
                    return row.getInt(14);
                }
                else {
                    return row.getInt(10);
                }
            }
            else {
                if ( row.getInt(14) <= row.getInt(9) ) {
                    return row.getInt(14);
                }
                else {
                    return row.getInt(9);
                }
            }
        }
        else {
            if ( isForward(row) ) {
                if ( row.get(14)==null )
                    return -1;
                return row.getInt(14);
            }
            else {
                if ( row.get(13)==null )
                    return -1;
                return row.getInt(13);
            }
        }
    }
}
