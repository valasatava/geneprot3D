package org.rcsb.genevariation.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.datastructures.VcfContainer;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.utils.VariationUtils;

public class MapToMutation implements FlatMapFunction <VcfContainer, Mutation> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7162653868949047763L;
	
	private Broadcast<RNApolymerase> polymeraseBroadcast;
	private Broadcast<List<GeneChromosomePosition>> chromosomePositions;
	
	public MapToMutation(Broadcast<RNApolymerase> polymeraseBroadcast, Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast) {
		this.polymeraseBroadcast = polymeraseBroadcast;
		this.chromosomePositions = transcriptsBroadcast;
	}

	@Override
	public Iterator<Mutation> call(VcfContainer vcfDatum) throws Exception {
		RNApolymerase polymerase = polymeraseBroadcast.getValue();
		polymerase.setChromosome(vcfDatum.getChromosome());
		
		boolean forward = true;
//		if (vcfDatum.getOrientation().equals("-")) {
//			forward = false;
//		}
		
		List<Mutation> mutations = new ArrayList<>();
		
		List<GeneChromosomePosition> gcps = chromosomePositions.getValue();
		for (GeneChromosomePosition cp : gcps) {
			if (cp.getChromosome().equals(vcfDatum.getChromosome())) {
				
				int mRNAPos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(vcfDatum.getPosition(), cp);
				if ( mRNAPos == -1 )
					continue;
				
				String codingSequence = polymerase.getCodingSequence(cp.getExonStarts(), cp.getExonEnds(), 
						cp.getCdsStart(), cp.getCdsEnd(), forward);
				String codon = polymerase.getCodon(mRNAPos, codingSequence);
				
				String mutBase = vcfDatum.getVariant();
				String codonM="";
				if (forward) {
					codonM = VariationUtils.mutateCodonForward(mRNAPos, codon, mutBase);
				}
				else {
					codonM = VariationUtils.mutateCodonReverse(mRNAPos, codon, mutBase);
				}

				Mutation mutation = new Mutation();
				mutation.setChromosomeName(vcfDatum.getChromosome());
				mutation.setPosition(vcfDatum.getPosition());
				mutation.setRefAminoAcid(Ribosome.getCodingAminoAcid(codon));
				mutation.setMutAminoAcid(Ribosome.getCodingAminoAcid(codonM));
				
				mutations.add(mutation);
			}
		}
		return mutations.iterator();
	}
}
