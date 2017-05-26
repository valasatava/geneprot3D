package org.rcsb.geneprot.common.utils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.*;

/**
 * Created by yana on 5/25/17.
 */
public class FileCustomUtils {

    /** A utility method to read JSON Array from local file
     *
     * @param file the JSON file
     * @return JSONArray with data
     * @throws Exception when FileNotFoundException, ParseException, or IOException happens
     */
    public static org.json.JSONArray readJsonArrayFromLocalFile(File file) throws Exception {

        JSONParser parser = new JSONParser();
        JSONArray data = (JSONArray) parser.parse(new FileReader(file));

        org.json.JSONArray json = new org.json.JSONArray(data.toJSONString());
        return json;

    }

    public static void extractTarGZ(InputStream in) throws IOException {

        int BUFFER_SIZE=1024;

        GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
        try (TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {
            TarArchiveEntry entry;

            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                /** If the entry is a directory, create the directory. **/
                if (entry.isDirectory()) {
                    File f = new File(entry.getName());
                    boolean created = f.mkdir();
                    if (!created) {
                        System.out.printf("Unable to create directory '%s', during extraction of archive contents.\n",
                                f.getAbsolutePath());
                    }
                } else {
                    int count;
                    byte data[] = new byte[BUFFER_SIZE];
                    FileOutputStream fos = new FileOutputStream(entry.getName(), false);
                    try (BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER_SIZE)) {
                        while ((count = tarIn.read(data, 0, BUFFER_SIZE)) != -1) {
                            dest.write(data, 0, count);
                        }
                    }
                }
            }
            System.out.println("Untar completed successfully!");
        }
    }
}
