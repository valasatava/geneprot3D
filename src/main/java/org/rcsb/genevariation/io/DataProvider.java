package org.rcsb.genevariation.io;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DataProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String projectHome = getUserhome()+"/data/genevariation/";
	
	public static String getUserhome() {
		return userHome;
	}

	public static String getProjecthome() {
		return projectHome;
	}

	public static Path getHumanGenomeMappingPath() {
		return Paths.get(getProjecthome()+"/parquet/hg38/");
	}
}
