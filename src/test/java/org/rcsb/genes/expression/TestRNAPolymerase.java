package org.rcsb.genes.expression;

import static org.junit.Assert.*;

import java.util.List;
import java.util.stream.Collectors;

import org.rcsb.geneprot.genes.expression.RNApolymerase;
import org.junit.Test;
import org.rcsb.geneprot.genes.datastructures.Transcript;
import org.rcsb.geneprot.genes.parsers.GenePredictionsParser;

public class TestRNAPolymerase {
	
	/**
	 * TestJoin that RNAPolymerase class correctly gets coding sequence.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testGettingCodingSequence() throws Exception {
		
		String chromosomeName = "chr16";
		String geneName = "HBA1"; // gene on the reverse DNA strand 
		String geneBankId = "NM_000558"; // GeneBank ID for the transcript used for testing (ENST00000320868)
		
		List<Transcript> gcps = GenePredictionsParser.getChromosomeMappings().stream()
				.filter(t -> t.getChromosomeName().equals(chromosomeName))
				.filter(t -> t.getGeneName().equals(geneName))
				.filter(t -> t.getGeneBankId().equals(geneBankId))
				.collect(Collectors.toList());
		Transcript transcript = gcps.get(0);

		String expected = "ATGGTGCTGTCTCCTGCCGACAAGACCAACGTCAAGGCCGCCTGGGGTAAGGTCGGCGCGCACGCTGGCG"+
						"AGTATGGTGCGGAGGCCCTGGAGAGGATGTTCCTGTCCTTCCCCACCACCAAGACCTACTTCCCGCACTT"+
						"CGACCTGAGCCACGGCTCTGCCCAGGTTAAGGGCCACGGCAAGAAGGTGGCCGACGCGCTGACCAACGCC"+
						"GTGGCGCACGTGGACGACATGCCCAACGCGCTGTCCGCCCTGAGCGACCTGCACGCGCACAAGCTTCGGG"+
						"TGGACCCGGTCAACTTCAAGCTCCTAAGCCACTGCCTGCTGGTGACCCTGGCCGCCCACCTCCCCGCCGA"+
						"GTTCACCCCTGCGGTGCACGCCTCCCTGGACAAGTTCCTGGCTTCTGTGAGCACCGTGCTGACCTCCAAA"+
						"TACCGTTAA";
		RNApolymerase rnap = new RNApolymerase();
		String actual = rnap.getCodingSequence(transcript);
		assertEquals(expected, actual);
	}
}
