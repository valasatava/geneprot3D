package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.constants.VariantType;

public class VariantImpl implements Variant {
	
	private String chromosome;
	private long position;
	private VariantType type;
	
	public VariantImpl() {}
	
	public VariantImpl (String chromosome, long position, VariantType type) {
		setChromosome(chromosome);
		setPosition(position);
		setType(type);
	}

	public String getChromosomeName() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}

	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}

	public VariantType getType() {
		return type;
	}
	public void setType(VariantType type) {
		this.type = type;
	}

	@Override
	public void setVariation(String ref, String alt) {
	}

	@Override
	public String getRefBase() {
		return "";
	}

	@Override
	public String getAltBase() {
		return "";
	}
}
