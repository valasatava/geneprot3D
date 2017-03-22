package exonscorrelation;

import java.io.Serializable;

public class ExonData implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8230108599766825720L;
	
	private String chromosome; // e.g., chr21
	private String geneName;
	
	private String geneBankId;
	private String ensemblId;
	
	private int start;
	private int end;
	
	private String orientation;
	private int offset;
	
	public ExonData() {
		
	}
	
	public ExonData(String chromosome, String geneName, int start, int end, String orientation) {
		setChromosome(chromosome);
		setGeneName(geneName);
		setStart(start);
		setEnd(end);
		setOrientation(orientation);
	}
	public String getChromosome() {
		return chromosome;
	}
	public void setChromosome(String chromosome) {
		this.chromosome = chromosome;
	}
	public String getGeneName() {
		return geneName;
	}
	public void setGeneName(String geneName) {
		this.geneName = geneName;
	}
	public String getGeneBankId() {
		return geneBankId;
	}
	public void setGeneBankId(String geneBankId) {
		this.geneBankId = geneBankId;
	}
	public String getEnsemblId() {
		return ensemblId;
	}
	public void setEnsemblId(String ensemblId) {
		this.ensemblId = ensemblId;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public String getOrientation() {
		return orientation;
	}
	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
}
