package org.rcsb.genevariation.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.GroupType;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.io.PDBFileParser;
import org.rcsb.genevariation.datastructures.MetalBindingResidue;
import org.rcsb.genevariation.utils.SaprkUtils;

public class MetalBindingDataProvider extends DataLocationProvider {

	private final static String path = getDataHome() + "external/metal_binding_residues/";
	private final static String pathParquetFile = getDataHome() + "metal_binding_residues.parquet";

	public static List<MetalBindingResidue> readMetalLigandsData() throws FileNotFoundException, IOException {

		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

		List<MetalBindingResidue> residues = new ArrayList<MetalBindingResidue>();

		PDBFileParser pdbparser = new PDBFileParser();

		// e.g 2zdo_A_HEM_200_FE_3559_Fe.lig
		for (int i = 0; i < listOfFiles.length; i++) {

			File fl = listOfFiles[i];

			if (fl.isFile()) {

				String filename = fl.getName();
				String[] cofactorInfo = filename.split("_");

				String pdbId = cofactorInfo[0];
				String cofactorName = cofactorInfo[2];
				String cofactorResNumber = cofactorInfo[3];
				String metalName = cofactorInfo[4];

				List<String> resKeys = new ArrayList<String>();

				String file = path + filename;
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {

					Structure structure=null;
					try {
						structure = pdbparser.parsePDBFile(br);
					} catch (Exception e) {	
						System.out.println("Parsing error: "+filename);
						continue;
					}
					
					for (Chain chain : structure.getChains()) {
						for (Group group : chain.getAtomGroups()) {

							String resName = group.getPDBName().trim();
							Integer resNumber = null;
							try {
								resNumber = Integer.valueOf(group.getResidueNumber().toString().trim());
							} catch (NumberFormatException e) {
								String st = group.getResidueNumber().toString();
								resNumber = Integer.valueOf(st.substring(0, st.length()-1).trim());
							}
							String chainId = group.getChainId().trim();
							GroupType type = group.getType();

							String key = resName + "_" + resNumber;
							if ((!resKeys.contains(key)) && (!resName.equals(metalName))) {
								MetalBindingResidue mbr = new MetalBindingResidue();
								mbr.setPdbId(pdbId.toUpperCase());
								mbr.setChainId(chainId);
								mbr.setResName(resName);
								mbr.setResNumber(Integer.valueOf(resNumber));
								mbr.setEndogenous(type);
								mbr.setCofactorName(cofactorName);
								mbr.setCofactorResNumber(Integer.valueOf(cofactorResNumber));
								mbr.setMetalName(metalName);
								mbr.setMetalResNumber(Integer.valueOf(cofactorResNumber));
								residues.add(mbr);
								resKeys.add(key);
							}
						}
					}
				}
			}
		}
		return residues;
	}

	public static void createParquetFile(List<MetalBindingResidue> residues) {
		String dataPath = getDataHome() + "metal_binding_residues.parquet";
		Dataset<Row> mydf = SaprkUtils.getSparkSession().createDataFrame(residues, MetalBindingResidue.class);
		mydf.write().mode(SaveMode.Overwrite).parquet(dataPath);
	}
	
	public static void createParquetFile() throws FileNotFoundException, IOException {
		List<MetalBindingResidue> residues = readMetalLigandsData();
		createParquetFile(residues);
	}
	
	public static Dataset<Row> readParquetFile() {
		 return SaprkUtils.getSparkSession().read().parquet(pathParquetFile);
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
				
		Dataset<Row> metalDF = readParquetFile();
		metalDF.createOrReplaceTempView("metals");
		metalDF.show();
	}
}