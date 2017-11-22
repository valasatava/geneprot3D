package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TranscriptToIsoform implements Serializable {

	private String transcriptId;
	private String transcriptName;
	private String moleculeId;

	private List<CoordinatesRange> codingCoordinates = new ArrayList<>();
	private List<CoordinatesRange> mRNACoordinates = new ArrayList<>();
	private List<CoordinatesRange> isoformCoordinates = new ArrayList<>();

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

	public List<CoordinatesRange> getCodingCoordinates() {
		return codingCoordinates;
	}

	public void setCodingCoordinates(List<CoordinatesRange> codingCoordinates) {
		this.codingCoordinates = codingCoordinates;
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

