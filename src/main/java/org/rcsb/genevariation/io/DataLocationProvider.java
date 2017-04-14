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

	// Collaboration projects
	private final static String exonsProject = getUserHome()+"/ishaan/";

	// Human genome
	private final static String humanGenomeLocation = getDataHome()
			+"hg38.2bit";
	private static final String genesPredictionURL = "http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";

	// Homology models data
	private final static String humanHomologyModelsLocation = getDataHome()
			+"parquet/human-homology-models";
	private final static String homologyModelsMappingLocation = getDataHome()
			+"parquet/homology-models-mapping-pc30";

	// Metal-binding data
	private final static String metalPDBdataLocation = getDataHome()
			+ "external/metal_binding_residues/";
	private final static String metalBindingMappingLocation = getDataHome()
			+ "parquet/metal_binding_residues";

	// Mappings
	private final static String hgMappingLocation = getUserHome()
			+ "/spark/parquet/humangenome/20170405/hg38/";
	private final static String uniprotPdbMappinlLocation = getDataHome()
			+ "/parquet/uniprotpdb/20161104/";


	//Gencode v.24 data
	private final static String gencodeProteinCodingDataLocation = getExonsProject()
			+"GENCODE_DATA/gencode.v24.CDS.protein_coding.gtf";

	private final static String gencodeGeneBankLocation = getExonsProject()
			+"MAPS/gencode.v24.CDS.protein_coding.gene_bank_mapping";

	private final static String gencodeUniprotLocation = DataLocationProvider.getExonsProject()
			+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping";

	private final static String gencodePDBLocation = DataLocationProvider.getExonsProject()
			+"MAPS/gencode.v24.CDS.protein_coding.pdb_mapping";

	private final static String gencodeHomologyModelsLocation = DataLocationProvider.getExonsProject()
			+"MAPS/gencode.v24.CDS.protein_coding.homology_mapping";


	// Correlated exons data
	private final static String exonsProteinCodingDataLocation = getExonsProject()
			+"GENCODE_DATA/NEW_EXONS/FDR0.gene.CDS.non_redundant";

	private final static String exonsGeneBankLocation = getExonsProject()
			+"MAPS/correlated_exons.gene_bank_mapping";

	private final static String exonsUniprotLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.uniprot_mapping";

	private final static String exonsPDBLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.pdb_mapping";

	private final static String exonsHomologyModelsLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.homology_mapping";

	// methods for this class

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
	public static String getHomologyModelsMappingLocation() {
		return homologyModelsMappingLocation;
	}

	public static  String getMetalPDBdataLocation() {
		return metalPDBdataLocation;
	}
	public static String getMetalBindingMappingLocation() {
		return metalBindingMappingLocation;
	}

	public static String getHgMappingLocation() {
		return hgMappingLocation;
	}
	public static String getUniprotPdbMappinlLocation() {
		return uniprotPdbMappinlLocation;
	}

	public static String getGencodeProteinCodingDataLocation() {
		return gencodeProteinCodingDataLocation;
	}
	public static String getGencodeGeneBankLocation() {
		return gencodeGeneBankLocation;
	}
	public static String getGencodeUniprotLocation() {
		return gencodeUniprotLocation;
	}
	public static String getGencodePDBLocation() {
		return gencodePDBLocation;
	}
	public static String getGencodeHomologyModelsLocation() {
		return gencodeHomologyModelsLocation;
	}

	public static String getExonsProteinCodingDataLocation() {
		return exonsProteinCodingDataLocation;
	}
	public static String getExonsGeneBankLocation() {
		return exonsGeneBankLocation;
	}
	public static String getExonsUniprotLocation() {
		return exonsUniprotLocation;
	}
	public static String getExonsPDBLocation() {
		return exonsPDBLocation;
	}
	public static String getExonsHomologyModelsLocation() {
		return exonsHomologyModelsLocation;
	}
}
