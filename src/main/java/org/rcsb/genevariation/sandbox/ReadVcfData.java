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
					String refBase = position.getRef();
					
					if ( pos >=10413531 ) {
						
					System.out.println(chr + " " + pos + " " + refBase + " " + bases.toString());
					try {
						System.out.println(twoBitParser.readBaseFromChromosome(chr, pos));
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					
//					SnpBean snpData = VcfDataConsumer.getPhaseSNP(position.getChromosome(), pos);
//					if (snpData.getPhase() >= 0) {
//						try {
//							String codon = twoBitParser.readCodonFromChromosome(chr, pos, snpData.getPhase());
//							
//							System.out.println(refBase);
//							System.out.println(codon);
//							
//							DNASequence dnaBases = new DNASequence(codon);
//							RNASequence rna = dnaBases.getRNASequence();
//							ProteinSequence aa = rna.getProteinSequence();
//							System.out.println(aa.getSequenceAsString());
//							
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
					System.out.println();
					}
				})
				.build();
		parser.parse();

	}
}
