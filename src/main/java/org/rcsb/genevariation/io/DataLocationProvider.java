package org.rcsb.genevariation.io;

import java.nio.file.Path;
import java.nio.file.Paths;

/** The class to set up the location of various data
 *
 * @author Yana Valasatava
 *
 */
public class DataLocationProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String dataHome = getUserHome()+"/data/genevariation/";

	private final static String exonsProject = getUserHome()+"/ishaan/";

	private final static String humanGenomeLocation = getDataHome()+"hg38.2bit";
	private static final String genesPredictionURL = "http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";

	private final static String humanHomologyModelsLocation = getDataHome()+"parquet/human-homology-models";

	public static String getUserHome() {
		return userHome;
	}
	public static String getDataHome() {
		return dataHome;
	}

	public static String getExonsProject() {
		return exonsProject;
	}

	/** Get a location of a human genome in .2bit format.
	 *
	 * @return path to locally stored .2bit file as String
	 */
	public static String getHumanGenomeLocation() {
		return humanGenomeLocation;
	}
	public static String getGenesPredictionURL() {
		return genesPredictionURL;
	}

	public static Path getHumanGenomeMappingPath() {
		return Paths.get(getDataHome()+"/parquet/hg38/");
	}

	public static String getHumanHomologyModelsLocation() {
		return humanHomologyModelsLocation;
	}
}
