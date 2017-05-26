package org.rcsb.geneprot.common.io;

import java.nio.file.Path;
import java.nio.file.Paths;

/** The class to set up the location of various data
 *
 * @author Yana Valasatava
 *
 */
public class DataLocationProvider {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String dataHome = getUserHome()+"/data/";
	private final static String projectsHome = getUserHome()+"/Projects/";

	// Collaboration projects
	private final static String exonsProject = getProjectsHome()+"coassociated_exons/";
	private final static String exonsProjectData = getExonsProject()+"EXONS_DATA/";
	private final static String exonsProjectResults = getExonsProject()+"RESULTS/";

	// Human genes
	private final static String humanGenomeLocation = getUserHome()+"/spark/parquet/humangenome/20170413/hg38.2bit";
	private static final String genesPredictionURL = "http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";

	// Homology models data
	private final static String humanHomologyModelsLocation = getDataHome()
			+"parquet/human-homology-models";
	private final static String humanHomologyCoordinatesLocation = getDataHome()
			+"structures/human-homology-models/";
	private final static String humanModelsJSONFileLocation = getDataHome()
			+"external/swissmodel/human_models.json";

	// Metal-binding data
	private final static String metalPDBdataLocation = getDataHome()
			+ "external/metalpdb/";
	private final static String metalBindingMappingLocation = getDataHome()
			+ "parquet/metal-binding-residues";

	// Mappings
	private final static String hgMappingLocation = getUserHome()
			+ "/spark/parquet/humangenome/20170413/hg38/";
	private final static String uniprotPdbMappinlLocation = getDataHome()
			+ "/parquet/uniprot-pdb/20161104/";

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
	private final static String gencodeStructuralMappingLocation = DataLocationProvider.getExonsProject()
			+"MAPS/gencode.v24.CDS.protein_coding.structural_mapping";

	// Correlated exons data
	private final static String exonsProteinCodingDataLocation = getExonsProject()
			+"EXONS_DATA/NEW_EXONS/FDR0.gene.CDS.non_redundant";

	private final static String exonsGeneBankLocation = getExonsProject()
			+"MAPS/correlated_exons.gene_bank_mapping";
	private final static String exonsUniprotLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.uniprot_mapping";
	private final static String exonsPDBLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.pdb_mapping";
	private final static String exonsHomologyModelsLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.homology_mapping";
	private final static String exonsStructuralMappingLocation = DataLocationProvider.getExonsProject()
			+"MAPS/correlated_exons.structural_mapping";

	// methods for this class

	public static String getUserHome() {
		return userHome;
	}
	public static String getDataHome() {
		return dataHome;
	}
	public static String getProjectsHome() {
		return projectsHome;
	}

	public static String getExonsProject() {
		return exonsProject;
	}
	public static String getExonsProjectData() {
		return exonsProjectData;
	}

	public static String getExonsProjectResults() {
		return exonsProjectResults;
	}

	/** Get a location of a human genes in .2bit format.
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
	public static String getHumanHomologyCoordinatesLocation() {
		return humanHomologyCoordinatesLocation;
	}

	public static String getHumanModelsJSONFileLocation() {
		return humanModelsJSONFileLocation;
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
	public static String getGencodeStructuralMappingLocation() {
		return gencodeStructuralMappingLocation;
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
	public static String getExonsStructuralMappingLocation() {
		return exonsStructuralMappingLocation;
	}
}
