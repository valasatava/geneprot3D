package org.rcsb.geneprot.genevariation.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genevariation.io.MetalBindingDataProvider;
import org.rcsb.geneprot.genevariation.io.VariantsDataProvider;

import java.io.IOException;

/**
 * TestJoin class
 * 
 * @author Yana Valasatava
 */
public class ReadSNPsParquet {

	private final static String path = DataLocationProvider.getDataHome() + "parquet/variations";
	
	public static void main(String[] args) throws IOException {
		
		System.out.println("started...");
		VariantsDataProvider vdp = new VariantsDataProvider();
        Dataset<Row> mutations = vdp.getMissenseVariationDF(path);
        mutations.createOrReplaceTempView("mutations");
        
        System.out.println("missense muttations are mapped to the protein sequence");
        
        Dataset<Row> uniprotpdb = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getUniprotPdbMappinlLocation());
        uniprotpdb.createOrReplaceTempView("uniprotpdb");
		//uniprotpdb.show();

        Dataset<Row> metals = MetalBindingDataProvider.readParquetFile();
        metals.createOrReplaceTempView("metals");
        
        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",  
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};		
		for (String chr : chromosomes) {
			
			System.out.println("getting the data for the chromosome "+ chr);
			Dataset<Row> chromMapping = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getHgMappingLocation()+chr);
			chromMapping.createOrReplaceTempView("hgmapping");
			//chromMapping.show();
			System.out.println("...done");
			
			// uniprotpdb.pdbId, uniprotpdb.pdbAtomPos as pdbResNum,
	        Dataset<Row> mutationsMapping = SparkUtils.getSparkSession().sql("select hgmapping.geneSymbol, mutations.geneBankId, hgmapping.chromosome, hgmapping.position, "
	        		+ "hgmapping.uniProtId, hgmapping.uniProtCanonicalPos, uniprotpdb.pdbId, uniprotpdb.chainId, uniprotpdb.pdbAtomPos, mutations.refAminoAcid, mutations.mutAminoAcid from mutations " +
	                "inner join hgmapping on ( hgmapping.chromosome = mutations.chromosomeName and hgmapping.position = mutations.position ) "+
	        		"left join uniprotpdb on (uniprotpdb.uniProtId = hgmapping.uniProtId and uniprotpdb.uniProtPos = hgmapping.uniProtCanonicalPos) order by position");
	        mutationsMapping.createOrReplaceTempView("mutationsMapping");
	        
	        Dataset<Row> newdf = SparkUtils.getSparkSession().sql("select mutationsMapping.geneSymbol, mutationsMapping.geneBankId, mutationsMapping.chromosome, mutationsMapping.position, "
	        		+ "mutationsMapping.uniProtId, mutationsMapping.uniProtCanonicalPos, mutationsMapping.pdbId, mutationsMapping.chainId, mutationsMapping.pdbAtomPos as pdbResNum, "
	        		+ "metals.resName, metals.cofactorName, metals.cofactorResNumber, mutationsMapping.refAminoAcid, mutationsMapping.mutAminoAcid "
	        		+ "from mutationsMapping left join metals on (mutationsMapping.pdbId=metals.pdbId and mutationsMapping.chainId=metals.chainId and mutationsMapping.pdbAtomPos=metals.resNumber)");
	        newdf.createOrReplaceTempView("mutationsmetals");
	        
	        Dataset<Row> mutationsMappingPdb = SparkUtils.getSparkSession().sql("select * from mutationsmetals where mutationsmetals.cofactorName is not null");
	        mutationsMappingPdb.createOrReplaceTempView("mappingPDB");
	        mutationsMappingPdb.show();
	        
	        System.out.println();
		}
	}
}
