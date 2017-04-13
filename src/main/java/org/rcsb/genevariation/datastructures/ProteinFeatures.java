package org.rcsb.genevariation.datastructures;

import org.rcsb.genevariation.datastructures.ExonSerializable;

import java.io.Serializable;

public class ProteinFeatures extends ExonSerializable implements Serializable {

	private static final long serialVersionUID = 5134392454874646410L;
	
	private int[] charge = new int[0];
	private int[] polarity = new int[0];
	private float[] hydropathy = new float[0];
	private float[] disorder = new float[0];
	
	public float[] getDisorder() {
		return disorder;
	}
	public void setDisorder(float[] disorder) {
		this.disorder = disorder;
	}
	public float[] getHydropathy() {
		return hydropathy;
	}
	public void setHydropathy(float[] hydropathy) {
		this.hydropathy = hydropathy;
	}
	public int[] getCharge() {
		return charge;
	}
	public void setCharge(int[] charge) {
		this.charge = charge;
	}
	public int[] getPolarity() {
		return polarity;
	}
	public void setPolarity(int[] polarity) {
		this.polarity = polarity;
	}
}
