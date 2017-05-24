package org.rcsb.genevariation.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {

    public static List<Integer> getIntegerListFromString(String lst, String sep){
		String[] spl = lst.split(sep);
		ArrayList<Integer> l = new ArrayList<Integer>();
		for (String s : spl){
			l.add(Integer.parseInt(s));
		}
		l.trimToSize();
		return l;
	}
	
    /**
     * A utility method used while reading JSON Array from URL.
     * @param rd
     * @return
     * @throws Exception
     */
    private static String read(Reader rd) throws Exception {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }
    
    /**
     * A utility method to read JSON Array from URL.
     * @param url
     * @return JSONArray
     * @throws Exception
     */
    public static JSONArray readJsonArrayFromUrl(String url) throws Exception {

        InputStream is = new URL(url).openStream();
        Charset ENCODING = StandardCharsets.UTF_8;
        BufferedReader rd = new BufferedReader(new InputStreamReader(is, ENCODING));
        String jsonText = read(rd);
        JSONArray json = new JSONArray(jsonText);
        is.close();

        return json;
    }

    public static void writeListToFile(List<String>results, String filename) throws IOException {

        FileWriter writer = new FileWriter(filename);
        for(String str: results) {
            writer.write(str+"\n");
        }
        writer.close();
    }
}
