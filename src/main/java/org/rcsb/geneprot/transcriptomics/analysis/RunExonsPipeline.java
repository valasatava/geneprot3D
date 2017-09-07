package org.rcsb.geneprot.transcriptomics.analysis;

import org.rcsb.geneprot.transcriptomics.pipeline.*;

public class RunExonsPipeline {

	public static void runGencode(String genomeName) throws Exception {

//		System.out.println("Ensemble to GeneBank mapping...");
//		ARunGeneBankMapping.runGencode(genomeName);
//		System.out.println("...done!");
//
//		System.out.println("Uniprot mapping...");
//		BRunUniprotMapping.runGencode(genomeName);
//		System.out.println("...done!");

		System.out.println("Mapping to PDB structures...");
		CRunPDBStructuresMapping.runGencode(genomeName);
		System.out.println("...done!");

		System.out.println("Mapping to homology structures...");
		DRunHomologyModelsMapping.runGencode(genomeName);
		System.out.println("...done!");

		System.out.println("Structural mapping...");
		EGetStructuralMapping.runGencode(genomeName);
		System.out.println("...done!");
	}

	public static void runCorrelatedExons() throws Exception {

		System.out.println("Ensemble to GeneBank mapping...");
		ARunGeneBankMapping.runCorrelatedExons();
		System.out.println("...done!");

		System.out.println("Uniprot mapping...");
		BRunUniprotMapping.runCorrelatedExons();
		System.out.println("...done!");

		System.out.println("Mapping to PDB structures...");
		CRunPDBStructuresMapping.runCorrelatedExons();
		System.out.println("...done!");

		System.out.println("Mapping to homology structures...");
		DRunHomologyModelsMapping.runCorrelatedExons();
		System.out.println("...done!");

		System.out.println("Structural mapping...");
		EGetStructuralMapping.runCorrelatedExons();
		System.out.println("...done!");
	}

	public static void main(String[] args) throws Exception {
		
		long start = System.nanoTime();
		runGencode("mouse");
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
