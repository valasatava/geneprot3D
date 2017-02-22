package org.rcsb.genevariation.sandbox;

class SPAType {

	String name;
	String value;

	private SPAType(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public static final SPAType DAN = new SPAType("Dan", "I'm the dog..");
	public static final SPAType VIC = new SPAType("Vic", "I'm the kitten..");

	public String toString() {
		return name + " | " + value;
	}

	public static void main(String args[]) {
		System.out.println(SPAType.DAN);
	}
}
