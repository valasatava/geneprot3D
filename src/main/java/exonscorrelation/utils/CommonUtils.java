package exonscorrelation.utils;

import org.apache.spark.sql.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
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
}
