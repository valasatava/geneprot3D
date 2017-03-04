package org.rcsb.genevariation.analysis;

import java.util.List;

import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.io.DataProvider;
import org.rcsb.genevariation.io.VariantsDataProvider;

public class RunOnKaviarData {
	
	public static void run() throws Exception {
		
		String filepath = DataProvider.getProjecthome() + "Kaviar-160204-Public/vcfs/Kaviar-160204-Public-hg38-trim.vcf.gz";
		
		long start = System.nanoTime();
		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCFWithSpark(filepath);
		List<Mutation> mutations = vdp.getSNPMutations();
		vdp.createVariationDataFrame(mutations, "Kaviar-160204-Public-hg38-trim.parquet");
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
	
	public static void main(String[] args) throws Exception {
		run();
	}
}
