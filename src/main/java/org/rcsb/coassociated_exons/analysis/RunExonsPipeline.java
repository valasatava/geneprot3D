package org.rcsb.coassociated_exons.analysis;

import org.rcsb.coassociated_exons.pipeline.*;

public class RunExonsPipeline {

	public static void runGencodeV24() throws Exception {

		System.out.println("Ensemble to GeneBank mapping...");
		ARunGeneBankMapping.runGencodeV24();
		System.out.println("...done!");

		System.out.println("Uniprot mapping...");
		BRunUniprotMapping.runGencodeV24();
		System.out.println("...done!");

		System.out.println("Mapping to PDB structures...");
		CRunPDBStructuresMapping.runGencodeV24();
		System.out.println("...done!");

		System.out.println("Mapping to homology structures...");
		DRunHomologyModelsMapping.runGencodeV24();
		System.out.println("...done!");

		System.out.println("Structural mapping...");
		EGetStructuralMapping.runGencodeV24();
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
		//runCorrelatedExons();
		runGencodeV24();
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
