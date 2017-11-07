package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Transcript implements Serializable {

	private String rnaSequenceIdentifier;
	private String proteinSequenceIdentifier;
	private String moleculeId;
	private String isoformId;

	private int transcriptionStart;
	private int transcriptionEnd;

	private int cdsStart;
	private int cdsEnd;

	private int exonsCount;
	private List<CoordinatesRange> cdsCoordinates = new ArrayList<>();
	private List<CoordinatesRange> mRNACoordinates = new ArrayList<>();
	private List<CoordinatesRange> proteinCoordinates = new ArrayList<>();

	private boolean match;
	private boolean hasAlternativeExons;
	private List<Boolean> alternativeExons;

	public String getRnaSequenceIdentifier() {
		return rnaSequenceIdentifier;
	}

	public void setRnaSequenceIdentifier(String rnaSequenceIdentifier) {
		this.rnaSequenceIdentifier = rnaSequenceIdentifier;
	}

	public String getProteinSequenceIdentifier() {
		return proteinSequenceIdentifier;
	}

	public void setProteinSequenceIdentifier(String proteinSequenceIdentifier) {
		this.proteinSequenceIdentifier = proteinSequenceIdentifier;
	}

	public String getMoleculeId() {
		return moleculeId;
	}

	public void setMoleculeId(String moleculeId) {
		this.moleculeId = moleculeId;
	}

	public String getIsoformId() {
		return isoformId;
	}

	public void setIsoformId(String isoformId) {
		this.isoformId = isoformId;
	}

	public int getTranscriptionStart() {
		return transcriptionStart;
	}

	public void setTranscriptionStart(int transcriptionStart) {
		this.transcriptionStart = transcriptionStart;
	}

	public int getTranscriptionEnd() {
		return transcriptionEnd;
	}

	public void setTranscriptionEnd(int transcriptionEnd) {
		this.transcriptionEnd = transcriptionEnd;
	}

	public int getCdsStart() {
		return cdsStart;
	}

	public void setCdsStart(int cdsStart) {
		this.cdsStart = cdsStart;
	}

	public int getCdsEnd() {
		return cdsEnd;
	}

	public void setCdsEnd(int cdsEnd) {
		this.cdsEnd = cdsEnd;
	}

	public int getExonsCount() {
		return exonsCount;
	}

	public void setExonsCount(int exonsCount) {
		this.exonsCount = exonsCount;
	}

	public List<CoordinatesRange> getCdsCoordinates() {
		return cdsCoordinates;
	}

	public void setCdsCoordinates(List<CoordinatesRange> cdsCoordinates) {
		this.cdsCoordinates = cdsCoordinates;
	}

	public List<CoordinatesRange> getmRNACoordinates() {
		return mRNACoordinates;
	}

	public void setmRNACoordinates(List<CoordinatesRange> mRNACoordinates) {
		this.mRNACoordinates = mRNACoordinates;
	}

	public List<CoordinatesRange> getProteinCoordinates() {
		return proteinCoordinates;
	}

	public void setProteinCoordinates(List<CoordinatesRange> proteinCoordinates) {
		this.proteinCoordinates = proteinCoordinates;
	}

	public boolean isMatch() {
		return match;
	}

	public void setMatch(boolean match) {
		this.match = match;
	}

	public boolean isHasAlternativeExons() {
		return hasAlternativeExons;
	}

	public void setHasAlternativeExons(boolean hasAlternativeExons) {
		this.hasAlternativeExons = hasAlternativeExons;
	}

	public List<Boolean> getAlternativeExons() {
		return alternativeExons;
	}

	public void setAlternativeExons(List<Boolean> alternativeExons) {
		this.alternativeExons = alternativeExons;
	}
}

