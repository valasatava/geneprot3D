package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tara on 3/1/16.
 */
public class GeneToUniProt implements Serializable {

    protected String chromosome;
    protected String geneId;
    protected String geneName;
    protected String orientation;
    protected String uniProtId;

    protected List<TranscriptToIsoform> isoforms = new ArrayList<>();

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getGeneId() {
        return geneId;
    }

    public void setGeneId(String geneId) {
        this.geneId = geneId;
    }

    public String getGeneName() {
        return geneName;
    }

    public void setGeneName(String geneName) {
        this.geneName = geneName;
    }

    public String getOrientation() {
        return orientation;
    }

    public void setOrientation(String orientation) {
        this.orientation = orientation;
    }

    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

    public List<TranscriptToIsoform> getIsoforms() {
        return isoforms;
    }

    public void setIsoforms(List<TranscriptToIsoform> isoforms) {
        this.isoforms = isoforms;
    }
}
