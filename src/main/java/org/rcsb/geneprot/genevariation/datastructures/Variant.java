package org.rcsb.geneprot.genevariation.datastructures;

import org.rcsb.geneprot.genevariation.constants.VariantType;

public class Variant implements VariantInterface {
	
	private String chromosome;
	private long position;
	private VariantType type;
	private boolean reverse;
	
	public Variant() {}
	
	public Variant (String chromosome, long position, VariantType type) {
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

	@Override
	public void setReverse(boolean reverse) {
		this.reverse = reverse;
	}

	@Override
	public boolean isReverse() {
		return reverse;
	}
}
