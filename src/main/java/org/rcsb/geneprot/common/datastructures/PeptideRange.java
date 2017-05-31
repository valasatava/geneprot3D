package org.rcsb.geneprot.common.datastructures;

import org.biojava.nbio.structure.Group;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by yana on 5/9/17.
 */
public class PeptideRange {

    private String chromosome;
    private String geneBankId;
    private String ensemblId;
    private String geneName;

    private int genomicCoordsStart=-1;
    private int genomicCoordsEnd=-1;

    private String uniProtId="";
    private int uniProtCoordsStart=-1;
    private int uniProtCoordsEnd=-1;

    private String structureId="";
    private boolean experimental;
    private int structCoordsStart=-1;
    private int structCoordsEnd=-1;

    private float resolution=99.0f;
    private List<Group> structure;

    private List<Integer> otherResidues = new ArrayList<>();
    private List<Integer> activeSiteResidues = new ArrayList<>();
    private List<Integer> phosphoSiteResidues = new ArrayList<>();

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
    public void setGeneName(String gene) {
        geneName = gene;
    }
    public String getGeneName(){
        return geneName;
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
    public int getStructuralCoordsStart() {
        return structCoordsStart;
    }
    public void setStructuralCoordsStart(int structCoordsStart) {
        this.structCoordsStart = structCoordsStart;
    }
    public int getStructuralCoordsEnd() {
        return structCoordsEnd;
    }
    public void setStructuralCoordsEnd(int structCoordsEnd) {
        this.structCoordsEnd = structCoordsEnd;
    }

    public String getPdbId() {
        if (experimental) {
            return getStructureId().split("_")[0];
        }
        else {
            return getStructureId().split(Pattern.quote("."))[0];
        }
    }
    public String getChainId() {
        if (experimental) {
            return getStructureId().split("_")[1];
        }
        else {
            return getStructureId().split(Pattern.quote("."))[2].split("_")[0];
        }
    }

    public float getResolution() {
        return resolution;
    }
    public void setResolution(float resolution) {
        this.resolution = resolution;
    }

    public int getStructureLength() {
        return (getStructuralCoordsEnd()-getStructuralCoordsStart());
    }
    public int getProteinSequenceLength() {
        return (getUniProtCoordsEnd()-getUniProtCoordsStart());
    }

    public void setStructure(List<Group> structure) {
        this.structure = structure;
    }
    public List<Group> getStructure() {
        return structure;
    }

    public List<Integer> getOtherResidues() {
        return otherResidues;
    }
    public void setOtherResidues(List<Integer> otherResidues) {
        this.otherResidues = otherResidues;
    }
    public void addOtherResidue(Integer otherResidue) {
        this.otherResidues.add(otherResidue);
    }

    public List<Integer> getActiveSiteResidues() {
        return activeSiteResidues;
    }
    public void setActiveSiteResidues(List<Integer> activeSiteResidues) {
        this.activeSiteResidues = activeSiteResidues;
    }
    public void addActiveSiteResidue(Integer activeSite) {
        this.activeSiteResidues.add(activeSite);
    }

    public List<Integer> getPhosphoSiteResidues() {
        return phosphoSiteResidues;
    }
    public void setPhosphoSiteResidues(List<Integer> phosphoSiteResidues) {
        this.phosphoSiteResidues = phosphoSiteResidues;
    }
    public void addPhosphoSiteResidue(Integer phosphoSite) {
        this.phosphoSiteResidues.add(phosphoSite);
    }
}