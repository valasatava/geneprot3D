package org.rcsb.correlatedexons;

import org.rcsb.correlatedexons.pipeline.*;

public class RunExonsPipeline {

	public static void runGencodeV24() throws Exception {

		ARunGeneBankMapping.runGencodeV24();
		BRunUniprotMapping.runGencodeV24();
		CRunPDBStructuresMapping.runGencodeV24();
		DRunHomologyModelsMapping.runGencodeV24();
	}

	public static void runCorrelatedExons() throws Exception {

//		ARunGeneBankMapping.runCorrelatedExons();
//		BRunUniprotMapping.runCorrelatedExons();
		CRunPDBStructuresMapping.runCorrelatedExons();
//		DRunHomologyModelsMapping.runCorrelatedExons();
		EGetStructuralMapping.runCorrelatedExons();
	}

	public static void main(String[] args) throws Exception {
		
		long start = System.nanoTime();
		runCorrelatedExons();
		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
