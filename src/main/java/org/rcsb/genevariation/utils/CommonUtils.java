package org.rcsb.genevariation.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

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
    private static String readAll(Reader rd) throws Exception {
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
     * @return
     * @throws Exception
     */
    public static JSONArray readJsonArrayFromUrl(String url) throws Exception {
    	
    	Charset ENCODING = StandardCharsets.UTF_8;
        InputStream is = new URL(url).openStream();
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, ENCODING));
            String jsonText = readAll(rd);
            JSONArray json = new JSONArray(jsonText);
            return json;
        } finally {
            is.close();
        }
    }
}
