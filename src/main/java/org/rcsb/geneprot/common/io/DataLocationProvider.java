package org.rcsb.geneprot.common.io;

import org.rcsb.geneprot.common.utils.CommonConstants;

import java.nio.file.Path;
import java.nio.file.Paths;

/** The class to set up the location of various data
 *
 * @author Yana Valasatava
 *
 */
public class DataLocationProvider {

	private static String genome = "human";
	public static void setGenome(String genomeName){
		genome = genomeName;
	}
	public static String getGenome(){
		return genome;
	}

	private final static String userHome = System.getProperty("user.home");
	private final static String dataHome = getUserHome()+"/data/";
	private final static String sparkHome = getUserHome() + "/spark/";
	private final static String projectsHome = getUserHome()+"/Projects/";

	// Collaboration projects
	private final static String exonsProject = getProjectsHome()+"coassociated_exons/";
	private final static String exonsProjectData = getExonsProject()+"EXONS_DATA/";
	private final static String exonsProjectResults = getExonsProject()+"RESULTS/";


	// =-=-= GENOME SEQUENCES =-=-=

	// Human genome sequence in the 2bit file format
	private final static String humanGenomeSequenceLocation = getUserHome()+"/spark/2bit/human/"+CommonConstants.HUMAN_GENOME_ASSEMBLY_GRCH38+".2bit";
	// Mouse genome sequence in the 2bit file format
	private final static String mouseGenomeSequenceLocation = getUserHome()+"/spark/2bit/mouse/"+ CommonConstants.MOUSE_GENOME_ASSEMBLY_GRCH38+".2bit";

	/** Get a location of a human genes in .2bit format.
	 *
	 * @return path to locally stored .2bit file as String
	 */
	public static String getGenomeLocation() {
		if ( getGenome().equals("human")) {
			return humanGenomeSequenceLocation;
		}
		else if ( getGenome().equals("mouse")) {
			return mouseGenomeSequenceLocation;
		}
		else { return null;}
	}

	// =-=-=-=-=-=-=-=-=-=-=-=-=

	// =-=-= GENOME ANNOTATION =-=-=

	private static final String humanGenomeAnnotationResource = "http://hgdownload.cse.ucsc.edu/goldenPath/"
			+CommonConstants.HUMAN_GENOME_ASSEMBLY_GRCH38+"/database/refFlat.txt.gz";
	public static String getHumanGenomeAnnotationResourceFromUCSC() {
		return humanGenomeAnnotationResource;
	}

	private static final String mouseGenomeAnnotationResource = "http://hgdownload.cse.ucsc.edu/goldenPath/"
			+CommonConstants.MOUSE_GENOME_ASSEMBLY_GRCH38+"/database/refFlat.txt.gz";
	public static String getMouseGenomeAnnotationResourceFromUCSC() {
		return mouseGenomeAnnotationResource;
	}

//	private static final String humanGenomeAnnotationResourceFromEnsembl = "http://ftp.ensembl.org/pub/release-90/gtf/homo_sapiens/Homo_sapiens.GRCh38.90.gtf.gz";
	private static final String humanGenomeAnnotationResourceFromEnsembl = "/Users/yana/Downloads/Homo_sapiens.GRCh38.90.gtf";
//	private static final String humanGenomeAnnotationResourceFromEnsembl = "/Users/yana/Downloads/WDR86.gtf";

	public static String getHumanGenomeAnnotationResourceFromEnsembl() {
		return humanGenomeAnnotationResourceFromEnsembl;
	}

	private static final String mouseGenomeAnnotationResourceFromEnsembl = "";
	public static String getMouseGenomeAnnotationResourceFromEnsembl() {
		return mouseGenomeAnnotationResourceFromEnsembl;
	}

	public static String getGenomeAnnotationResource(int taxonomyId, String format) {

		if (format.equals("refFlat")) {
			if (taxonomyId == 9606) {
				return getHumanGenomeAnnotationResourceFromUCSC();
			} else if (taxonomyId == 10090) {
				return getMouseGenomeAnnotationResourceFromUCSC();
			}
		} else if (format.equals("gtf")) {
			if (taxonomyId == 9606) {
				return getHumanGenomeAnnotationResourceFromEnsembl();
			} else if (taxonomyId == 10090) {
				return getMouseGenomeAnnotationResourceFromEnsembl();
			}
		}
		return null;
	}
// =-=-=-=-=-=-=-=-=-=-=-=-=

	private static final String humanUniProtMapping = "ftp://anonymous:pwd@141.161.180.197/ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping//by_organism/HUMAN_9606_idmapping_selected.tab.gz";
	public static String getUniProtMappingResource(int taxonomyId) {
		if (taxonomyId == 9606)
			return humanUniProtMapping;
		return null;
	}

	// Metal-binding data
	private final static String metalPDBdataLocation = getDataHome()
			+ "external/metalpdb/";
	private final static String metalBindingMappingLocation = getDataHome()
			+ "parquet/metal-binding-residues";

	// Mappings
	private final static String hgMappingLocation = getSparkHome()
			+ "parquet/humangenome/20170413/hg38";
	private final static String uniprotPdbMappinlLocation = getDataHome()
			+ "/parquet/uniprot-pdb/20161104/";

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
	public static String getSparkHome() {return sparkHome;}

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
		return getExonsProject()+"RESULTS/"+getGenome()+"/";
	}


	public static Path getHumanGenomeMappingPath() {
		return Paths.get(getDataHome()+"/parquet/hg38/");
	}

	public static String getHomologyModelsLocation() {
		return getDataHome() +"parquet/"+getGenome()+"-homology-models";
	}
	public static String getHomologyModelsCoordinatesLocation() {
		return getDataHome()+"structures/"+getGenome()+"-homology-models/";
	}
	public static String getHomologyModelsJSONFileLocation() {
		return getDataHome()+"external/swissmodel/"+getGenome()+"_models.json";
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
		return getExonsProject() +"GENCODE_DATA/"+getGenome()+"/gencode.annotation.gtf";
	}
	public static String getGencodeGeneBankLocation() {
		return getExonsProject() +"MAPS/"+getGenome()+"/gencode.gene_bank_mapping";
	}
	public static String getGencodeUniprotLocation() {
		return getExonsProject() +"MAPS/"+getGenome()+"/gencode.uniprot_mapping";
	}
	public static String getGencodePDBLocation() {
		return getExonsProject() +"MAPS/"+getGenome()+"/gencode.pdb_mapping";
	}
	public static String getGencodeHomologyMappingLocation() {
		return getExonsProject() +"MAPS/"+getGenome()+"/gencode.homology_mapping";
	}
	public static String getGencodeStructuralMappingLocation() {
		return getExonsProject() +"MAPS/"+getGenome()+"/gencode.structural_mapping";
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
