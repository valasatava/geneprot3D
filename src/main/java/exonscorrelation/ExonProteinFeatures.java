package exonscorrelation;

import org.rcsb.genevariation.datastructures.ExonSerializable;

import java.io.Serializable;

public class ExonProteinFeatures extends ExonSerializable implements Serializable {

	private static final long serialVersionUID = 5134392454874646410L;
	
	private int[] charge;
	private int[] polarity;
	private float[] hydropathy;
	private float[] disorder;
	
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
