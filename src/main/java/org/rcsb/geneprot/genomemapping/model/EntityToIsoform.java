package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class EntityToIsoform implements Serializable {

    private String uniProtId;
    private String moleculeId;

    private String entryId;
    private String entityId;
    private String chainId;

    private List<CoordinatesRange> isoformCoordinates = new ArrayList<>();
    private List<CoordinatesRange> structureCoordinates = new ArrayList<>();

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

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getChainId() {
        return chainId;
    }

    public void setChainId(String chainId) {
        this.chainId = chainId;
    }

    public List<CoordinatesRange> getIsoformCoordinates() {
        return isoformCoordinates;
    }

    public void setIsoformCoordinates(List<CoordinatesRange> isoformCoordinates) {
        this.isoformCoordinates = isoformCoordinates;
    }

    public List<CoordinatesRange> getStructureCoordinates() {
        return structureCoordinates;
    }

    public void setStructureCoordinates(List<CoordinatesRange> structureCoordinates) {
        this.structureCoordinates = structureCoordinates;
    }
}
