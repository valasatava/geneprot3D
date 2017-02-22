package org.rcsb.genevariation.datastructures;

import java.util.ArrayList;
import java.util.List;

public class Gene {
	
	private String chromosome;
	private String name;

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
