package org.rcsb.geneprot.transcriptomics.datastructures;

/**
 * Created by yana on 5/25/17.
 */
public class ExonBoundariesPair {

    private String chromosome;
    private String exon1;
    private String exon2;

    public ExonBoundariesPair() {}

    public ExonBoundariesPair(String chromosome, String exon1, String exon2) {
        this.chromosome=chromosome;
        this.exon1=exon1;
        this.exon2=exon2;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getExon1() {
        return exon1;
    }

    public void setExon1(String exon1) {
        this.exon1 = exon1;
    }

    public String getExon2() {
        return exon2;
    }

    public void setExon2(String exon2) {
        this.exon2 = exon2;
    }
}
