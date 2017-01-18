package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.RNASequence;
import org.pharmgkb.parser.vcf.VcfParser;

public class ReadVcfData {

	private final static String userHome = System.getProperty("user.home");

	public static void main(String[] args) throws Exception {

		String path = userHome+"/data/genevariation/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf";
		Path file = Paths.get(path);

		VcfDataConsumer.setSpark();
		//		VcfDataConsumer.readUniprot();
		VcfDataConsumer.readChromosome("21");
		
		TwoBitGeneDataConsumer twoBitParser = new TwoBitGeneDataConsumer();
		
		
		VcfParser parser = new VcfParser
				.Builder()
				.fromFile(file)
				.parseWith((metadata, position, sampleData) -> {		        	
					
					String chr = position.getChromosome();
					long pos = position.getPosition();
					List<String> bases = position.getAltBases();
					
					if ( pos >=10413531 ) {
					System.out.println(chr +
							" " + pos+
							" " + bases.toString());
					
					int phase = VcfDataConsumer.getPhaseSNP(position.getChromosome(), pos);
					if (phase >= 0) {
						try {
							String codon = twoBitParser.readCodonFromChromosome(chr, pos, phase);
							System.out.println(codon);
							
							DNASequence dnaBases = new DNASequence(codon);
							RNASequence rna = dnaBases.getRNASequence();
							ProteinSequence aa = rna.getProteinSequence();
							System.out.println(aa.getSequenceAsString());
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println();
					}
				})
				.build();
		parser.parse();

	}
}
