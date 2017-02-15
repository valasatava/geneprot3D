//package org.rcsb.genevariation.sandbox;
//
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.List;
//
//import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
//import org.biojava.nbio.core.sequence.DNASequence;
//import org.biojava.nbio.core.sequence.ProteinSequence;
//import org.biojava.nbio.core.sequence.RNASequence;
//import org.pharmgkb.parser.vcf.VcfParser;
//import org.rcsb.genevariation.datastructures.SNP;
//
//public class ReadVcfData {
//
//	private final static String userHome = System.getProperty("user.home");
//	static String innerChr = "";
//
//	public static void setChrPosition(String chr) {
//		innerChr = chr;
//	}
//
//	public static String getChrPosition() {
//		return innerChr;
//	}
//
//	public static RNASequence transcript(String sequence) throws CompoundNotFoundException {
//		DNASequence dna = new DNASequence(sequence);
//		return dna.getRNASequence();
//	}
//
//	public static ProteinSequence translate(RNASequence sequence) throws CompoundNotFoundException {
//		return sequence.getProteinSequence();
//	}
//
//	public static String mutatePosition(String codonRef, int phase, String orientation, String variation) {
//		String codonVar;
//		if (orientation.equals("+")) {
//			codonVar = codonRef.substring(0, phase) + variation + codonRef.substring(phase + 1, 3);
//		} else {
//			codonVar = codonRef.substring(0, 3 - phase) + variation + codonRef.substring(3 - phase, 3);
//		}
//		return codonVar;
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		String path = userHome + "/data/genevariation/common_all_20161122.vcf";
//		Path file = Paths.get(path);
//
//		//VcfDataConsumer.setSpark();
//		// VcfDataConsumer.readUniprot();
//
//		TwoBitGeneDataConsumer twoBitParser = new TwoBitGeneDataConsumer();
//
//		VcfParser parser = new VcfParser.Builder().fromFile(file).parseWith((metadata, position, sampleData) -> {
//
//			String chr = position.getChromosome();
//			if (!chr.equals(getChrPosition())) {
//				VcfDataConsumer.readChromosome(chr);
//				setChrPosition(chr);
//			}
//
//			long pos = position.getPosition();
//			List<String> bases = position.getAltBases();
//			String refBase = position.getRef();
//
//			// if a simple SNP -> mutate
//
//			System.out.println(chr + " " + pos + " " + refBase + " " + bases.toString());
//
//			if ( (refBase.length() == 1) && (bases.size() == 1) && (!bases.get(0).equals(".")) && (bases.get(0).length() == 1)) {
//
//				SNP snpData = VcfDataConsumer.getSNPData(position.getChromosome(), pos);
//				
//				if (snpData != null) {
//					
//					System.out.println(snpData.getChromosomeName());
//					System.out.println(snpData.getPhase());
//					System.out.println(snpData.getOrientation());
//					
//					if (snpData.getPhase() >= 0) {
//						try {
//							String codonRef = twoBitParser.readCodonFromChromosome(chr, pos, snpData.getPhase(),
//									snpData.getOrientation());
//
//							System.out.println(refBase);
//							System.out.println(codonRef);
//
//							RNASequence rnaRef = transcript(codonRef);
//							ProteinSequence aaRef = translate(rnaRef);
//							System.out.println("Reference AA: " + aaRef.getSequenceAsString());
//
//							String codonVar = mutatePosition(codonRef, snpData.getPhase(), snpData.getOrientation(),
//									bases.get(0));
//							System.out.println(codonVar);
//
//							RNASequence rnaVar = transcript(codonVar);
//							ProteinSequence aaVar = translate(rnaVar);
//							System.out.println("Variation AA: " + aaVar.getSequenceAsString());
//
//							System.out.println();
//
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//					System.out.println();
//				}
//			}
//		}).build();
//		parser.parse();
//	}
//}
