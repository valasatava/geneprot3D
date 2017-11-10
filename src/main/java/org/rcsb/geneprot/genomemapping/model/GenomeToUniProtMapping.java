package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tara on 3/1/16.
 */
public class GenomeToUniProtMapping implements Serializable {

    protected String chromosome;
    protected String geneName;
    protected String orientation;
    protected String uniProtId;

    protected List<TranscriptMapping> transcripts = new ArrayList<>();

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
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

    public List<TranscriptMapping> getTranscripts() {
        return transcripts;
    }

    public void setTranscripts(List<TranscriptMapping> transcripts) {
        this.transcripts = transcripts;
    }
}
