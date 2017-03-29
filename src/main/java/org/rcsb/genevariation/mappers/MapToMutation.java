//package org.rcsb.genevariation.mappers;
//
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.sql.Row;
//import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
//import org.biojava.nbio.genome.util.ChromosomeMappingTools;
//import org.rcsb.genevariation.datastructures.Mutation;
//import org.rcsb.genevariation.expression.RNApolymerase;
//import org.rcsb.genevariation.expression.Ribosome;
//import org.rcsb.genevariation.utils.VariationUtils;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//public class MapToMutation implements FlatMapFunction <Row, Mutation> {
//
//	/**
//	 *
//	 */
//	private static final long serialVersionUID = -7162653868949047763L;
//
//	private Broadcast<RNApolymerase> polymeraseBroadcast;
//	private Broadcast<List<GeneChromosomePosition>> chromosomePositions;
//
//	public MapToMutation(Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast) {
//		this.chromosomePositions = transcriptsBroadcast;
//	}
//
//	@Override
//	public Iterator<Mutation> call(Row row) throws Exception {
//
//		if (row.get(0).toString().equals("TPTE") && (row.get(1).toString().equals("rs760594483"))) {
//			System.out.println();
//		}
//
//        ChromosomeMappingTools mapper = new ChromosomeMappingTools();
//
//		File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
//		mapper.readGenome(f);
//		mapper.setChromosome((String) row.get(2).toString());
//
//		boolean forward = true;
//		if (row.get(8).toString().equals("-")) {
//			forward = false;
//		}
//
//		List<Mutation> mutations = new ArrayList<>();
//
//		List<GeneChromosomePosition> gcps = chromosomePositions.getValue();
//		for (GeneChromosomePosition cp : gcps) {
//			if (cp.getChromosome().equals(row.get(2).toString()) && cp.getGeneName().equals(row.get(0).toString()) && (cp.getCdsStart()<=row.getInt(3) && cp.getCdsEnd()>=row.getInt(3))) {
//
//                int mRNAPos = 0;
//                try {
//                    mRNAPos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(row.getInt(3), cp);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                if ( mRNAPos == -1 )
//					continue;
//
//                String transcript = null;
//                try {
//                    transcript = mapper.getTranscriptSequence(cp.getExonStarts(), cp.getExonEnds(),
//                            cp.getCdsStart(), cp.getCdsEnd(), row.get(8).toString().charAt(0));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                try {
//                    if (transcript.equals(""))
//                        continue;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                RNApolymerase polymerase = new RNApolymerase();
//                String codon = null;
//                try {
//                    codon = polymerase.getCodon(mRNAPos, transcript);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                String mutBase;
//				if (forward) {
//					mutBase = row.get(7).toString();
//				}
//				else {
//					mutBase = VariationUtils.reverseComplimentaryBase(row.get(7).toString());
//				}
//
//				String codonM="";
//				if (forward) {
//					codonM = VariationUtils.mutateCodonForward(mRNAPos, codon, mutBase);
//				}
//				else {
//					codonM = VariationUtils.mutateCodonReverse(mRNAPos, codon, mutBase);
//				}
//
//				Mutation mutation = new Mutation();
//				mutation.setChromosomeName(row.get(2).toString());
//				mutation.setPosition(Long.valueOf(row.get(3).toString()));
//				mutation.setRefAminoAcid(Ribosome.getProteinSequence(codon));
//				mutation.setMutAminoAcid(Ribosome.getProteinSequence(codonM));
//
//				mutations.add(mutation);
//			}
//		}
//        mapper.close();
//
//		return mutations.iterator();
//	}
//}
