package org.rcsb.geneprot.common.datastructures;

import org.biojava.nbio.structure.GroupType;

import java.io.Serializable;

/**
 * 
 */
public class MetalBindingResidue implements Serializable {
	
	private static final long serialVersionUID = -2624853599877696735L;
	
	private String pdbId;
	private String chainId;
	
	private String resName;
	private int resNumber;
	
	private String cofactorName;
	private int cofactorResNumber;
	
	private String metalName;
	private int metalResNumber;
	
	//private String donorAtomName;
	private boolean endogenous;
	
	public String getPdbId() {
		return pdbId;
	}
	public String getChainId() {
		return chainId;
	}
	public String getResName() {
		return resName;
	}
	public int getResNumber() {
		return resNumber;
	}
	public String getCofactorName() {
		return cofactorName;
	}
	public int getCofactorResNumber() {
		return cofactorResNumber;
	}
	public String getMetalName() {
		return metalName;
	}
	public int getMetalResNumber() {
		return metalResNumber;
	}
//	public String getDonorAtomName() {
//		return donorAtomName;
//	}
	public boolean isEndogenous() {
		return endogenous;
	}
	public void setPdbId(String pdbId) {
		this.pdbId = pdbId;
	}
	public void setChainId(String chainId) {
		this.chainId = chainId;
	}
	public void setResName(String resName) {
		this.resName = resName;
	}
	public void setResNumber(int resNumber) {
		this.resNumber = resNumber;
	}
	public void setCofactorName(String cofactorName) {
		this.cofactorName = cofactorName;
	}
	public void setCofactorResNumber(int cofactorResNumber) {
		this.cofactorResNumber = cofactorResNumber;
	}
	public void setMetalName(String metalName) {
		this.metalName = metalName;
	}
	public void setMetalResNumber(int metalResNumber) {
		this.metalResNumber = metalResNumber;
	}
//	public void setDonorAtomName(String donorAtomName) {
//		this.donorAtomName = donorAtomName;
//	}
	public void setEndogenous(boolean endogenous) {
		this.endogenous = endogenous;
	}
	
	public void setEndogenous(GroupType type) {
		if (type.toString().equals("AMINOACID")) {
			setEndogenous(true);
		}
		else {
			setEndogenous(false);
		}
	}
}
