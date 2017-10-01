package org.rcsb.geneprot.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by ap3 on 26/05/2016.
 */
public class Parameters {

    private static final Logger logger = LoggerFactory.getLogger(Parameters.class);

    public static String propFileName = "app.properties";
    private static Properties properties;

    public static String userHome = System.getProperty("user.home");

    public static String workDirectoryName = "spark";
    public static String getWorkDirectory() {
        return userHome + "/" + workDirectoryName;
    }

    private static Properties getPropertiesObject(String propertiesFileName)
    {
        Properties props = null;
        try {
            InputStream propstream = new FileInputStream(propertiesFileName);
            props = new Properties();
            props.load(propstream);
        } catch (IOException var6) {
            String msg = "Something went wrong reading the file " + propertiesFileName + ", although the file was reported as existing";
            throw new RuntimeException(msg);
        }

        return props;
    }

    public static Properties getProperties() {
        if (properties != null) {
            return properties;
        }
        properties = getPropertiesObject(propFileName);
        return properties;
    }
}
