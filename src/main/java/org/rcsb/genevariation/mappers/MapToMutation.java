package org.rcsb.genevariation.mappers;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.utils.VariationUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapToMutation implements FlatMapFunction <Row, Mutation> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7162653868949047763L;

	private Broadcast<RNApolymerase> polymeraseBroadcast;
	private Broadcast<List<GeneChromosomePosition>> chromosomePositions;
	
	public MapToMutation(Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast) {
		this.chromosomePositions = transcriptsBroadcast;
	}

	@Override
	public Iterator<Mutation> call(Row row) throws Exception {

		RNApolymerase polymerase = new RNApolymerase((String) row.get(2).toString());

		boolean forward = true;
		if (row.get(8).toString().equals("-")) {
			forward = false;
		}
		
		List<Mutation> mutations = new ArrayList<>();
		
		List<GeneChromosomePosition> gcps = chromosomePositions.getValue();
		for (GeneChromosomePosition cp : gcps) {
			if (cp.getChromosome().equals(row.get(2).toString())) {
				
				int mRNAPos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(Integer.valueOf(row.get(3).toString()), cp);
				if ( mRNAPos == -1 )
					continue;

				String codingSequence = polymerase.getCodingSequence(cp.getExonStarts(), cp.getExonEnds(), 
						cp.getCdsStart(), cp.getCdsEnd(), forward);
				if (codingSequence.equals(""))
					continue;
				String codon = polymerase.getCodon(mRNAPos, codingSequence);

				String mutBase;
				if (forward) {
					mutBase = row.get(7).toString();
				}
				else {
					mutBase = VariationUtils.reverseComplimentaryBase(row.get(7).toString());
				}

				String codonM="";
				if (forward) {
					codonM = VariationUtils.mutateCodonForward(mRNAPos, codon, mutBase);
				}
				else {
					codonM = VariationUtils.mutateCodonReverse(mRNAPos, codon, mutBase);
				}

				Mutation mutation = null;
				try {
					mutation = new Mutation();
					mutation.setChromosomeName(row.get(2).toString());
					mutation.setPosition(Long.valueOf(row.get(3).toString()));
					mutation.setRefAminoAcid(Ribosome.getProteinSequence(codon));
					mutation.setMutAminoAcid(Ribosome.getProteinSequence(codonM));
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (CompoundNotFoundException e) {
					e.printStackTrace();
				}

				//System.out.println(mutation.getChromosomeName() + " " + mutation.getPosition());

				mutations.add(mutation);
			}
		}
		return mutations.iterator();
	}
}
