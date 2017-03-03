package org.rcsb.genevariation.io;

public class DataProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String projectHome = getUserhome()+"/data/genevariation/";
	
	public static String getUserhome() {
		return userHome;
	}

	public static String getProjecthome() {
		return projectHome;
	}
}
