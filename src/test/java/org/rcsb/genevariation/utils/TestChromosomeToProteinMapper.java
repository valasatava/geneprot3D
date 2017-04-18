package org.rcsb.genevariation.utils;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.junit.Test;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.parser.GenePredictionsParser;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestChromosomeToProteinMapper {
	
	/** TestJoin that  class correctly gets the transcript sequence on forward strand.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testGettingTranscriptForward() throws Exception {
		
		String chromosomeName = "chr16";
		String geneName = "HBA1"; // gene on the forward DNA strand 
		String geneBankId = "NM_000558"; // GeneBank ID for the transcript used for testing (Ensemble ID: ENST00000320868)
		
		List<Transcript> gcps = GenePredictionsParser.getChromosomeMappings().stream()
				.filter(t -> t.getChromosomeName().equals(chromosomeName))
				.filter(t -> t.getGeneName().equals(geneName))
				.filter(t -> t.getGeneBankId().equals(geneBankId))
				.collect(Collectors.toList());
		Transcript transcript = gcps.get(0);

		DNASequence expected = new DNASequence( "ATGGTGCTGTCTCCTGCCGACAAGACCAACGTCAAGGCCGCCTGGGGTAAGGTCGGCGCGCACGCTGGCG"+
						"AGTATGGTGCGGAGGCCCTGGAGAGGATGTTCCTGTCCTTCCCCACCACCAAGACCTACTTCCCGCACTT"+
						"CGACCTGAGCCACGGCTCTGCCCAGGTTAAGGGCCACGGCAAGAAGGTGGCCGACGCGCTGACCAACGCC"+
						"GTGGCGCACGTGGACGACATGCCCAACGCGCTGTCCGCCCTGAGCGACCTGCACGCGCACAAGCTTCGGG"+
						"TGGACCCGGTCAACTTCAAGCTCCTAAGCCACTGCCTGCTGGTGACCCTGGCCGCCCACCTCCCCGCCGA"+
						"GTTCACCCCTGCGGTGCACGCCTCCCTGGACAAGTTCCTGGCTTCTGTGAGCACCGTGCTGACCTCCAAA"+
						"TACCGTTAA");
		
		ChromosomeMappingTools mapper = new ChromosomeMappingTools();
		
		File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
		TwoBitFacade twoBitFacade = new TwoBitFacade(f);
		
		Character orientation='+';
		if (transcript.getOrientation().equals(StrandOrientation.REVERSE)){
			orientation='-';
		}

		DNASequence actual = mapper.getTranscriptDNASequence(twoBitFacade, chromosomeName, transcript.getExonStarts(), transcript.getExonEnds(),
				transcript.getCodingStart(), transcript.getCodingEnd(), orientation);
		
		assertEquals(expected, actual);
	}

	/** TestJoin that  class correctly gets the transcript sequence on reverse strand.
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testGettingTranscriptReverse() throws Exception {
		
		String chromosomeName = "chr17";
		String geneName = "YBX2"; // gene on the reverse DNA strand 
		String geneBankId = "NM_015982"; // GeneBank ID for the transcript used for testing (Ensemble ID: ENST00000007699)
		
		List<Transcript> gcps = GenePredictionsParser.getChromosomeMappings().stream()
				.filter(t -> t.getChromosomeName().equals(chromosomeName))
				.filter(t -> t.getGeneName().equals(geneName))
				.filter(t -> t.getGeneBankId().equals(geneBankId))
				.collect(Collectors.toList());
		Transcript transcript = gcps.get(0);

		DNASequence expected = new DNASequence("ATGAGCGAGGTGGAGGCGGCAGCGGGGGCTACAGCGGTCCCCGCGGCGACGGTGCCCGCGACGGCGGCAG"+
							"GGGTGGTAGCGGTGGTGGTACCGGTGCCCGCAGGGGAGCCGCAGAAAGGCGGCGGGGCGGGCGGCGGGGG"+
							"CGGAGCCGCCTCGGGCCCCGCTGCTGGGACCCCCTCGGCGCCGGGCTCCCGCACCCCTGGCAATCCGGCG"+
							"ACGGCGGTCTCGGGAACCCCCGCCCCCCCGGCCCGGAGTCAGGCGGACAAGCCGGTGCTGGCAATCCAAG"+
							"TCCTGGGCACTGTCAAATGGTTCAACGTCCGGAATGGTTACGGATTCATCAACAGGAATGACACCAAGGA"+
							"AGATGTCTTTGTTCACCAGACAGCTATTAAAAGAAACAACCCCAGGAAGTTTCTGCGCAGCGTTGGAGAT"+
							"GGGGAGACTGTGGAATTTGATGTCGTGGAAGGAGAGAAGGGCGCAGAAGCCACTAATGTAACTGGGCCTG"+
							"GGGGAGTACCCGTGAAGGGCAGCCGTTATGCCCCCAACCGACGTAAGTCCCGCCGATTCATCCCCCGGCC"+
							"TCCCTCAGTTGCCCCACCACCCATGGTGGCAGAGATCCCCTCGGCGGGGACAGGACCTGGCAGTAAAGGG"+
							"GAGCGGGCTGAAGACTCTGGGCAACGGCCCCGACGATGGTGCCCCCCACCCTTCTTCTACCGACGGCGGT"+
							"TTGTGCGAGGCCCCCGGCCTCCCAACCAGCAGCAGCCTATAGAGGGCACTGACAGGGTAGAACCCAAAGA"+
							"GACAGCCCCATTGGAGGGGCACCAACAGCAGGGAGATGAGCGAGTCCCCCCGCCCAGATTCCGGCCCAGG"+
							"TACCGAAGGCCTTTCCGCCCCAGGCCACGCCAGCAGCCTACCACAGAAGGTGGGGATGGTGAGACCAAGC"+
							"CCAGCCAAGGTCCCGCTGATGGTTCCCGGCCTGAGCCCCAGCGCCCACGAAACCGCCCCTACTTCCAGCG"+
							"GAGACGGCAGCAGGCCCCTGGCCCCCAGCAGGCCCCTGGCCCCCGGCAGCCCGCAGCCCCTGAGACCTCA"+
							"GCCCCTGTCAACAGTGGGGACCCCACCACCACCATCCTGGAGTGA");
		
		ChromosomeMappingTools mapper = new ChromosomeMappingTools();
		
		File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
		TwoBitFacade twoBitFacade = new TwoBitFacade(f);
		
		Character orientation='+';
		if (transcript.getOrientation().equals(StrandOrientation.REVERSE)){
			orientation='-';
		}

		DNASequence actual = mapper.getTranscriptDNASequence(twoBitFacade, chromosomeName, transcript.getExonStarts(), transcript.getExonEnds(),
				transcript.getCodingStart(), transcript.getCodingEnd(), orientation);
		
		assertEquals(expected, actual);
	}
}
