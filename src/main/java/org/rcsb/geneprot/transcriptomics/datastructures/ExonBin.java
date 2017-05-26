package org.rcsb.geneprot.transcriptomics.datastructures;

import org.rcsb.geneprot.genes.datastructures.ExonSerializable;

/**
 * Created by yana on 4/19/17.
 */
public class ExonBin extends ExonSerializable {

    private String uniProtId;

    private int uniProtStart;
    private int uniProtEnd;

    private boolean structure=false;

    private String pdbId="";
    private String chainId="";

    private String coordinates="";

    private int pdbStart;
    private int pdbEnd;

    public ExonBin(String chromosome, String geneName, int start, int end, String orientation) {
        super(chromosome, geneName, start, end, orientation);
    }

    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

    public int getUniProtStart() {
        return uniProtStart;
    }

    public void setUniProtStart(int uniProtStart) {
        this.uniProtStart = uniProtStart;
    }

    public int getUniProtEnd() {
        return uniProtEnd;
    }

    public void setUniProtEnd(int uniProtEnd) {
        this.uniProtEnd = uniProtEnd;
    }

    public boolean isStructure() {
        return structure;
    }

    public void setStructure(boolean structure) {
        this.structure = structure;
    }

    public String getPdbId() {
        return pdbId;
    }

    public void setPdbId(String pdbId) {
        this.pdbId = pdbId;
    }

    public String getChainId() {
        return chainId;
    }

    public void setChainId(String chainId) {
        this.chainId = chainId;
    }

    public String getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(String coordinates) {
        this.coordinates = coordinates;
    }

    public int getPdbStart() {
        return pdbStart;
    }

    public void setPdbStart(int pdbStart) {
        this.pdbStart = pdbStart;
    }

    public int getPdbEnd() {
        return pdbEnd;
    }

    public void setPdbEnd(int pdbEnd) {
        this.pdbEnd = pdbEnd;
    }
}
