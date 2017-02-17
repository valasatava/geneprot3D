package org.rcsb.genevariation.sandbox;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.io.GenomeDataProvider;
import org.rcsb.genevariation.io.PDBDfDataProvider;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.utils.DataProviderFilterChromosome;
import org.rcsb.genevariation.utils.DataProviderFilterSNP;
import org.rcsb.genevariation.utils.IDataProviderFilter;

public class ReadVcfData {

	private final static String userHome = System.getProperty("user.home");

	public static void main(String[] args) throws Exception {
		
		System.out.println("Started...");
		
		long start = System.nanoTime();
		
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		System.out.println("Job ID:"+timeStamp);
		
		String path = userHome + "/data/genevariation/common_and_clinical_20170130.vcf";
		Path file = Paths.get(path);

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		System.out.println("Time to read VCF file: " + (System.nanoTime() - start)/1E9 + " sec.");
		long start2 = System.nanoTime();
		
		String chrN = "17";
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chrN);
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();
		
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterVar));
		
		
		GenomeDataProvider.readGenome();
		GenomeDataProvider.setChromosome(chrN);
		
		Iterator<Variant> vars = vdp.getAllVariants();
		while (vars.hasNext()) {
			Variant v = vars.next();		
		}
		
		System.out.println("Time to filter the variation data: " + (System.nanoTime() - start2)/1E9 + " sec.");
		long start3 = System.nanoTime();
		
		Dataset<Row> chromosome = PDBDfDataProvider.readChromosome(chrN);
		
		System.out.println("Time to get chromosome data: " + (System.nanoTime() - start3)/1E9 + " sec.");
		
		
		
		System.out.println("DONE!");
		System.out.println("Total time: " + (System.nanoTime() - start)/1E9 + " sec.");
		
	}
}
