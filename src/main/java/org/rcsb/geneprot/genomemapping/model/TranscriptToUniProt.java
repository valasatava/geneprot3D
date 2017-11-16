package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TranscriptToUniProt implements Serializable {

	private String transcriptId;
	private String transcriptName;

	private int exonsCount;
	private CoordinatesRange transcriptionCoordinates;
	private List<CoordinatesRange> urtCoordinates = new ArrayList<>();
	private List<CoordinatesRange> codingCoordinates = new ArrayList<>();
	private List<CoordinatesRange> exonCoordinates = new ArrayList<>();
	private List<CoordinatesRange> mRNACoordinates = new ArrayList<>();
	private List<CoordinatesRange> isoformCoordinates = new ArrayList<>();

	private boolean hasAlternativeExons;
	private List<Boolean> alternativeExons;

	private String moleculeId;
	private String sequence;
	private String sequenceStatus;

	public String getTranscriptId() {
		return transcriptId;
	}

	public void setTranscriptId(String transcriptId) {
		this.transcriptId = transcriptId;
	}

	public String getTranscriptName() {
		return transcriptName;
	}

	public void setTranscriptName(String transcriptName) {
		this.transcriptName = transcriptName;
	}

	public int getExonsCount() {
		return exonsCount;
	}

	public void setExonsCount(int exonsCount) {
		this.exonsCount = exonsCount;
	}

	public CoordinatesRange getTranscriptionCoordinates() {
		return transcriptionCoordinates;
	}

	public void setTranscriptionCoordinates(CoordinatesRange transcriptionCoordinates) {
		this.transcriptionCoordinates = transcriptionCoordinates;
	}

	public List<CoordinatesRange> getUrtCoordinates() {
		return urtCoordinates;
	}

	public void setUrtCoordinates(List<CoordinatesRange> urtCoordinates) {
		this.urtCoordinates = urtCoordinates;
	}

	public List<CoordinatesRange> getCodingCoordinates() {
		return codingCoordinates;
	}

	public void setCodingCoordinates(List<CoordinatesRange> codingCoordinates) {
		this.codingCoordinates = codingCoordinates;
	}

	public List<CoordinatesRange> getExonCoordinates() {
		return exonCoordinates;
	}

	public void setExonCoordinates(List<CoordinatesRange> exonCoordinates) {
		this.exonCoordinates = exonCoordinates;
	}

	public List<CoordinatesRange> getmRNACoordinates() {
		return mRNACoordinates;
	}

	public void setmRNACoordinates(List<CoordinatesRange> mRNACoordinates) {
		this.mRNACoordinates = mRNACoordinates;
	}

	public List<CoordinatesRange> getIsoformCoordinates() {
		return isoformCoordinates;
	}

	public void setIsoformCoordinates(List<CoordinatesRange> isoformCoordinates) {
		this.isoformCoordinates = isoformCoordinates;
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

	public String getMoleculeId() {
		return moleculeId;
	}

	public void setMoleculeId(String moleculeId) {
		this.moleculeId = moleculeId;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	public String getSequenceStatus() {
		return sequenceStatus;
	}

	public void setSequenceStatus(String sequenceStatus) {
		this.sequenceStatus = sequenceStatus;
	}
}

