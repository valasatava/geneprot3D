package org.rcsb.genevariation.sandbox;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.RNASequence;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.utils.DataProviderFilterChromosome;
import org.rcsb.genevariation.utils.DataProviderFilterSNP;
import org.rcsb.genevariation.utils.IDataProviderFilter;

public class ReadVcfData {

	private final static String userHome = System.getProperty("user.home");
	static String innerChr = "";

	public static void setChrPosition(String chr) {
		innerChr = chr;
	}

	public static String getChrPosition() {
		return innerChr;
	}

	public static RNASequence transcript(String sequence) throws CompoundNotFoundException {
		DNASequence dna = new DNASequence(sequence);
		return dna.getRNASequence();
	}

	public static ProteinSequence translate(RNASequence sequence) throws CompoundNotFoundException {
		return sequence.getProteinSequence();
	}

	public static String mutatePosition(String codonRef, int phase, String orientation, String variation) {
		String codonVar;
		if (orientation.equals("+")) {
			codonVar = codonRef.substring(0, phase) + variation + codonRef.substring(phase + 1, 3);
		} else {
			codonVar = codonRef.substring(0, 3 - phase) + variation + codonRef.substring(3 - phase, 3);
		}
		return codonVar;
	}

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
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(1);
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();
		
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterVar));
		
		System.out.println("Time to filter the data: " + (System.nanoTime() - start2)/1E9 + " sec.");
		
		Dataset<Row> vars = vdp.getVariantsDataframe();
		System.out.println(vars.count());
		
		System.out.println("DONE!");
		System.out.println("Total time: " + (System.nanoTime() - start)/1E9 + " sec.");
		
	}
}
