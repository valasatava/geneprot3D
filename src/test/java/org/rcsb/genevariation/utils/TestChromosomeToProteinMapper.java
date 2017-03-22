package org.rcsb.genevariation.utils;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.parser.GenePredictionsParser;

public class TestChromosomeToProteinMapper {
	
	/** Test that  class correctly gets the transcript sequence.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testGettingTranscript() throws Exception {
		
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
		
		ChromosomeToProteinMapper mapper = new ChromosomeToProteinMapper();
		
		mapper.setGenomeURI(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
		mapper.readGenome();
		mapper.setChromosome(chromosomeName);
		
		Character orientation='+';
		if (transcript.getOrientation().equals(StrandOrientation.REVERSE)){
			orientation='-';
		}
		
		String actual = mapper.getTranscriptSequence(transcript.getExonStarts(), transcript.getExonEnds(),
				transcript.getCodingStart(), transcript.getCodingEnd(), orientation);
		
		assertEquals(expected, actual);
	}

}
