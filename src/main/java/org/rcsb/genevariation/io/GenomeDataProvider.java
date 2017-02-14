package org.rcsb.genevariation.io;

public class GenomeDataProvider {
	
	private static String base;
	
	public GenomeDataProvider() { }
	
	public GenomeDataProvider(String path) {
		setBase(path);
	}
	private void setBase(String path) {
		base = path;
	}

}
