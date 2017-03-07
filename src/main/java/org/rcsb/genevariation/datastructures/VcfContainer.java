package org.rcsb.genevariation.datastructures;

import java.io.Serializable;

public class VcfContainer  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 742291184200630347L;
	
	String chromosome;
	Integer genomicPosition;
	String dbSnpID;
	String original;
	String variant;
	String orientation;

	public String getChromosome() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = "chr"+String.valueOf(chromosome);
	}
	public Integer getPosition() {
		return genomicPosition;
	}
	public void setPosition(Integer position) {
		this.genomicPosition = position;
	}
	public String getDbSnpID() {
		return dbSnpID;
	}
	public void setDbSnpID(String dbSnpID) {
		this.dbSnpID = dbSnpID;
	}
	public String getOriginal() {
		return original;
	}
	public void setOriginal(String original) {
		this.original = original;
	}
	public String getVariant() {
		return variant;
	}
	public void setVariant(String variant) {
		this.variant = variant;
	}

	public String getOrientation() {
		return orientation;
	}
	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}
}
