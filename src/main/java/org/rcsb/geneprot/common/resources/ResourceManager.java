package org.rcsb.geneprot.common.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Yana Valasatava on 8/7/17.
 */
public class ResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    private final static String userHome = System.getProperty("user.home");
    private static String TMP_RESOURCE_DIRECTORY = userHome+"/spark/tmp/";

    private Map<String, String> resources;
    List<File> answer = new ArrayList();

    public Map<String, String> getResources() {
        return resources;
    }

    public void setResources(Map<String, String> resources)
    {
        this.resources = resources;
    }

    public List<File> getRequiredExternalResourcesList()
    {
        if(answer.size() == 0) {
            this.downloadRequiredExternalResources();
        }
        return answer;
    }

    private File downloadFile(URL website, String filePath) throws IOException
    {
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        File f = new File(filePath);
        FileOutputStream fos = new FileOutputStream(f);
        fos.getChannel().transferFrom(rbc, 0L, 9223372036854775807L);

        return f;
    }

    public boolean downloadRequiredExternalResources()
    {
        Map<String, String> resources = this.getResources();

        for (String fileName : resources.keySet())
        {
            URL url = null;
            try {
                url = new URL(resources.get(fileName));
            } catch (MalformedURLException e) {
                logger.error(e.getMessage(), e.getCause());
                return false;
            }

            File fileResource = new File(TMP_RESOURCE_DIRECTORY + fileName);

            if(fileResource == null || !fileResource.exists()) {
                try {
                    fileResource = this.downloadFile(url, fileResource.getAbsolutePath());
                    answer.add(fileResource);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e.getCause());
                    return false;
                }
            } else {
                logger.info("Found an external resource in file system: " + fileResource.getAbsolutePath());
                answer.add(fileResource);
            }
        }
       return true;
    }

    public String getLocalResourcePath(String resourceName) throws FileNotFoundException {
        if (resources.keySet().contains(resourceName))
            return TMP_RESOURCE_DIRECTORY + resourceName;
        else
            throw new FileNotFoundException("The resource is not found locally. Set the Resource Manager and download the file: " + resourceName);
    }
}