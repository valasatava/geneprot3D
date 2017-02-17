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
public class TestExon {
	
	/**
	 * Test the method to set a forward DNA sequence
	 * @throws CompoundNotFoundException 
	 */
	@Test
	public void testSetForwardDNASequence() throws CompoundNotFoundException {
		
		String dnaSequence = "ATTCG";
		mRNA exon = new mRNA();
		exon.setOrientation(StrandOrientation.FORWARD);
		exon.setDNASequence(dnaSequence);
		
		String exonSequence = exon.getDNASequenceAsString();
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
				
		mRNA exon = new mRNA();
		exon.setOrientation(StrandOrientation.REVERSE);
		exon.setDNASequence(dnaSequence);
		
		String exonSequence = exon.getDNASequenceAsString();
		assertEquals(dnaComplementRev, exonSequence);
	}
}
