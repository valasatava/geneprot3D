package org.rcsb.geneprot.gencode.dao;

import org.rcsb.geneprot.gencode.resources.ResourceManager;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Created by Yana Valasatava on 8/7/17.
 */
public class MetadataDAO {

    public static Map<String, String> getBinaryMapping(InputStream inStream, int indexTrId, int indexId) throws IOException
    {
        Map<String, String> map = new HashMap<>();
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
        while((line = reader.readLine()) != null) {
            String[] fields = line.split(Pattern.quote("\t"));
            if (fields.length>1) {
                String key = fields[indexTrId].split(Pattern.quote("."))[0];
                String value = fields[indexId];
                map.put(key, value);
            }
            else {
                System.out.println(line);
            }
        }
        return map;
    }

    public static Map<String, String> getGeneSymbols()  throws IOException
    {
        ResourceManager rm = new ResourceManager();
        Map<String, String> resources = new HashMap<>();
        resources.put("geneSymbols", "ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_mouse/release_M14/gencode.vM14.metadata.MGI.gz");
        rm.setResources(resources);

        List<File> externalResources = rm.getRequiredExternalResourcesList();
        File f = externalResources.get(0);

        InputStream fileStream = new FileInputStream(f);
        InputStream gzipStream = new GZIPInputStream(fileStream);

        Map<String, String> geneSymbols = getBinaryMapping(gzipStream, 0, 1);

        return geneSymbols;
    }

    public static Map<String, String> getGeneSwissProt()  throws IOException
    {
        ResourceManager rm = new ResourceManager();
        Map<String, String> resources = new HashMap<>();
        resources.put("geneSwissProt", "ftp://ftp.sanger.ac.uk/pub/gencode/Gencode_mouse/release_M14/gencode.vM14.metadata.SwissProt.gz");
        rm.setResources(resources);

        List<File> externalResources = rm.getRequiredExternalResourcesList();
        File f = externalResources.get(0);

        InputStream fileStream = new FileInputStream(f);
        InputStream gzipStream = new GZIPInputStream(fileStream);

        Map<String, String> geneSwissProt = getBinaryMapping(gzipStream, 0, 1);

        return geneSwissProt;
    }

    public static void main(String[] args) {
    }
}
