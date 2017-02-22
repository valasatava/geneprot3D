package org.rcsb.genevariation.datastructures;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.rcsb.genevariation.constants.StrandOrientation;

/**
 * Class to test the Exon class
 * 
 * @author Yana Valasatava
 */
public class TestTranscript {
	
	/**
	 * Test the method to set a forward DNA sequence
	 * @throws CompoundNotFoundException 
	 */
	@Test
	public void testSetForwardDNASequence() throws CompoundNotFoundException {
		
		String dnaSequence = "ATTCG";
		Transcript transcript = new Transcript();
		transcript.setOrientation(StrandOrientation.FORWARD);
		transcript.setDNASequence(dnaSequence);
		
		String exonSequence = transcript.getDNASequenceAsString();
		assertEquals(dnaSequence, exonSequence);
	}
	
	/**
	 * Test the method to set a reverse DNA sequence
	 * @throws CompoundNotFoundException 
	 */
	@Test
	public void testSetReverseDNASequence() throws CompoundNotFoundException {
		
		String dnaSequence = "ATTCG";
		//String dnaComplement = "TAAGC";
		String dnaComplementRev = "CGAAT";
				
		Transcript transcript = new Transcript();
		transcript.setOrientation(StrandOrientation.REVERSE);
		transcript.setDNASequence(dnaSequence);
		
		String exonSequence = transcript.getDNASequenceAsString();
		assertEquals(dnaComplementRev, exonSequence);
	}
}
