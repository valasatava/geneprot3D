package org.rcsb.geneprot.common.utils;


import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class DBConnectionUtils {

	static final Logger logger = LoggerFactory.getLogger(DBConnectionUtils.class);
	private static Properties props = Parameters.getProperties();
    
	public static String MONGO_DB_IP = props.getProperty("mongodb.ip");
	public static String MONGO_DB_NAME = props.getProperty("mongodb.db.name");
	
	
    private static BasicDataSource dsDest, dsSourcePDB, dsSourceUniprot;
    private static HashMap<String, String> sourceDBOptionsPDB;
    private static HashMap<String, String> sourceDBOptionsUniprot = getSourceDBOptionsUniprot();
    private static HashMap<String, String> mongoDBOptions;
    
    private static HashMap<String, String> destinationDBOptions;
    
    
    public static BasicDataSource getSourceDataSourcePDB()
    {
        if(dsSourcePDB != null)
            return dsSourcePDB;

        dsSourcePDB = new BasicDataSource();
        dsSourcePDB.setDriverClassName(props.getProperty("source.db.driver"));
        dsSourcePDB.setUrl(props.getProperty("source.db.url"));
        dsSourcePDB.setUsername(props.getProperty("source.db.username"));
        dsSourcePDB.setPassword(props.getProperty("source.db.password"));

        return dsSourcePDB;
    }

    public static HashMap<String, String> getSourceDBOptionsPDB()
    {
    	if(sourceDBOptionsPDB != null)
            return sourceDBOptionsPDB;

        sourceDBOptionsPDB = new HashMap<>();
        sourceDBOptionsPDB.put("driver", props.getProperty("source.db.driver"));
        sourceDBOptionsPDB.put("url", props.getProperty("source.db.url"));
        sourceDBOptionsPDB.put("user", props.getProperty("source.db.username"));
        sourceDBOptionsPDB.put("password", props.getProperty("source.db.password"));

        return sourceDBOptionsPDB;
    }

    public static BasicDataSource getSourceDataSourceUniprot()
    {
        if(dsSourceUniprot != null)
            return dsSourceUniprot;

        dsSourceUniprot = new BasicDataSource();
        dsSourceUniprot.setDriverClassName(props.getProperty("source.uniprot.db.driver"));
        dsSourceUniprot.setUrl(props.getProperty("source.db.uniprot.url"));
        dsSourceUniprot.setUsername(props.getProperty("source.db.uniprot.username"));
        dsSourceUniprot.setPassword(props.getProperty("source.db.uniprot.password"));

        return dsSourceUniprot;
    }

    public static HashMap<String, String> getSourceDBOptionsUniprot()
    {
        if(sourceDBOptionsUniprot != null)
            return sourceDBOptionsUniprot;

        sourceDBOptionsUniprot = new HashMap<>();
        sourceDBOptionsUniprot.put("driver", props.getProperty("source.db.uniprot.driver"));
        sourceDBOptionsUniprot.put("url", props.getProperty("source.db.uniprot.url"));
        sourceDBOptionsUniprot.put("user", props.getProperty("source.db.uniprot.username"));
        sourceDBOptionsUniprot.put("password", props.getProperty("source.db.uniprot.password"));

        return sourceDBOptionsUniprot;
    }

    public static BasicDataSource getDestinationDataSource()
    {
        if(dsDest != null)
            return dsDest;

        dsDest = new BasicDataSource();
        dsDest.setDriverClassName(props.getProperty("destination.db.driver"));
        dsDest.setUrl(props.getProperty("destination.db.url"));
        dsDest.setUsername(props.getProperty("destination.db.username"));
        dsDest.setPassword(props.getProperty("destination.db.password"));

        return dsDest;
    }

    public static HashMap<String, String> getDestinationDBOptions()
    {
    	if(destinationDBOptions != null)
            return destinationDBOptions;
    	
        destinationDBOptions = new HashMap<>();
        destinationDBOptions.put("driver", props.getProperty("destination.db.driver"));
        destinationDBOptions.put("url", props.getProperty("destination.db.url"));
        destinationDBOptions.put("user", props.getProperty("destination.db.username"));
        destinationDBOptions.put("password", props.getProperty("destination.db.password"));

        return destinationDBOptions;
    }
    
    public static HashMap<String, String> getMongoDBOptions()
    {
    	if(mongoDBOptions != null)
            return mongoDBOptions;
    	
    	mongoDBOptions = new HashMap<String, String>();
    	mongoDBOptions.put("spark.mongodb.input.uri","mongodb://"+MONGO_DB_IP+"/"+MONGO_DB_NAME);
    	mongoDBOptions.put("spark.mongodb.output.uri","mongodb://"+MONGO_DB_IP+"/"+MONGO_DB_NAME);
    	
        return mongoDBOptions;
    }
    

}