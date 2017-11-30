package org.rcsb.geneprot.genomemapping.models;

/**
 * Created by Yana Valasatava on 11/28/17.
 */
public class IsoformPosition {

    protected String uniProtId;
    protected String moleculeId;
    protected int coordinate;
    protected char letter;

    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

    public String getMoleculeId() {
        return moleculeId;
    }

    public void setMoleculeId(String moleculeId) {
        this.moleculeId = moleculeId;
    }

    public int getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(int coordinate) {
        this.coordinate = coordinate;
    }

    public char getLetter() {
        return letter;
    }

    public void setLetter(char letter) {
        this.letter = letter;
    }
}
