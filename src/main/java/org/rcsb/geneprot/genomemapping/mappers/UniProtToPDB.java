package org.rcsb.geneprot.genomemapping.mappers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class UniProtToPDB implements Serializable {

    private String uniProtId;
    private String entryId;

    private List<EntityToIsoform> isoforms = new ArrayList<>();

    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public List<EntityToIsoform> getIsoforms() {
        return isoforms;
    }

    public void setIsoforms(List<EntityToIsoform> isoforms) {
        this.isoforms = isoforms;
    }
}
