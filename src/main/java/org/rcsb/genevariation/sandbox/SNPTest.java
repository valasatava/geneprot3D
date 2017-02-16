package org.rcsb.genevariation.sandbox;

import java.io.Serializable;

/**
 * Created by ali on 12/5/16.
 */
public class SNPTest implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -705271078484375955L;
	
	private int CHROM;

    public int getCHROM() {
        return CHROM;
    }

    public void setCHROM(int CHROM) {
        this.CHROM = CHROM;
    }
}
