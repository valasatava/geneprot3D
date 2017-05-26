package org.rcsb.geneprot.genes.datastructures;

import org.rcsb.geneprot.genes.constants.StrandOrientation;

import java.util.ArrayList;
import java.util.List;

public class Gene {
	
	private String chromosome;
	private String name;
	private String ensembleId;

	private StrandOrientation orientation;

	private List<Transcript> transcripts = new ArrayList<Transcript>();
	
	public String getChromosome() {
		return chromosome;
	}
	
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public String getEnsembleId() {
		return ensembleId;
	}

	public void setEnsembleId(String ensembleId) {
		this.ensembleId = ensembleId;
	}

	public StrandOrientation getOrientation() {
		return orientation;
	}

	public void setOrientation(StrandOrientation orientation) {
		this.orientation = orientation;
	}

	public List<Transcript> getTranscripts() {
		return transcripts;
	}
	
	public void addTranscript(Transcript transcript) {
		transcripts.add(transcript);
	}
	
	public void setTranscripts(List<Transcript> transcripts) {
		this.transcripts = transcripts;
	}
}
