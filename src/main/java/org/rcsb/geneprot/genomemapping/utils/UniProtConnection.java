package org.rcsb.geneprot.genomemapping.utils;

/**
 * Created by Yana Valasatava on 11/16/17.
 */
public class UniProtConnection {

    private static String server = "ftp.uniprot.org";
    private static int port = 21;
    private static String user = "anonymous";
    private static String pass = "anonymous@141.161.180.197";

    private static String idMappingLocation = "/pub/databases/uniprot/current_release/knowledgebase/idmapping//by_organism/";
    private static String humanIdMappingFile = "HUMAN_9606_idmapping_selected.tab.gz";


    public static String getServer() {
        return server;
    }

    public static int getPort() {
        return port;
    }

    public static String getUser() {
        return user;
    }

    public static String getPass() {
        return pass;
    }

    public static String getIdMappingLocation() {
        return idMappingLocation;
    }

    public static String getHumanIdMappingFile() {
        return humanIdMappingFile;
    }
}
