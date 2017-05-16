package org.rcsb.genevariation.datastructures;

/**
 * Created by yana on 5/9/17.
 */
public class PeptideRange {

    private String chromosome;
    private String geneBankId;
    private String ensemblId;
    private int genomicCoordsStart=-1;
    private int genomicCoordsEnd=-1;

    private String uniProtId;
    private int uniProtCoordsStart=-1;
    private int uniProtCoordsEnd=-1;

    private String structureId;
    private boolean experimental;
    private int structCoordsStart=-1;
    private int structCoordsEnd=-1;

    public String getChromosome() {
        return chromosome;
    }
    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
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
    public int getGenomicCoordsStart() {
        return genomicCoordsStart;
    }
    public void setGenomicCoordsStart(int genomicCoordsStart) {
        this.genomicCoordsStart = genomicCoordsStart;
    }
    public int getGenomicCoordsEnd() {
        return genomicCoordsEnd;
    }
    public void setGenomicCoordsEnd(int genomicCoordsEnd) {
        this.genomicCoordsEnd = genomicCoordsEnd;
    }

    public String getUniProtId() {
        return uniProtId;
    }
    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }
    public int getUniProtCoordsStart() {
        return uniProtCoordsStart;
    }
    public void setUniProtCoordsStart(int uniProtCoordsStart) {
        this.uniProtCoordsStart = uniProtCoordsStart;
    }
    public int getUniProtCoordsEnd() {
        return uniProtCoordsEnd;
    }
    public void setUniProtCoordsEnd(int uniProtCoordsEnd) {
        this.uniProtCoordsEnd = uniProtCoordsEnd;
    }

    public String getStructureId() {
        return structureId;
    }
    public void setStructureId(String structureId) {
        this.structureId = structureId;
    }
    public boolean isExperimental() {
        return experimental;
    }
    public void setExperimental(boolean experimental) {
        this.experimental = experimental;
    }
    public int getStructCoordsStart() {
        return structCoordsStart;
    }
    public void setStructuralCoordsStart(int structCoordsStart) {
        this.structCoordsStart = structCoordsStart;
    }
    public int getStructCoordsEnd() {
        return structCoordsEnd;
    }
    public void setStructuralCoordsEnd(int structCoordsEnd) {
        this.structCoordsEnd = structCoordsEnd;
    }
}