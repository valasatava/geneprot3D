package org.rcsb.geneprot.genomemapping.models;

/**
 * Created by Yana Valasatava on 11/28/17.
 */
public class Position {

    protected GenomicPosition genomicPosition;
    protected IsoformPosition isoformPosition;
    protected StructurePosition structurePosition;

    public GenomicPosition getGenomicPosition() {
        return genomicPosition;
    }

    public void setGenomicPosition(GenomicPosition genomicPosition) {
        this.genomicPosition = genomicPosition;
    }

    public IsoformPosition getIsoformPosition() {
        return isoformPosition;
    }

    public void setIsoformPosition(IsoformPosition isoformPosition) {
        this.isoformPosition = isoformPosition;
    }

    public StructurePosition getStructurePosition() {
        return structurePosition;
    }

    public void setStructurePosition(StructurePosition structurePosition) {
        this.structurePosition = structurePosition;
    }
}
