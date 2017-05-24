package org.rcsb.genevariation.mappers;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.uniprot.isoform.IsoformMapper;
import org.spark_project.guava.primitives.Ints;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by yana on 4/21/17.
 */
public class UniprotToModelCoordinatesMapper {

    private int from;
    private int to;

    private String alignment;
    private String coordinates;

    private int[] uniprotCoordinates;
    private int[] modelCoordinates;

    public void setFrom(int pos) {
        from = pos;
    }

    public void setTo(int pos) {
        to = pos;
    }

    public void setAlignment(String alignmentString) {
        alignment = alignmentString;
    }

    public String getAlignment() {
        return alignment;
    }

    public void setTemplate(String template) {
        coordinates = DataLocationProvider.getHumanHomologyCoordinatesLocation()
                +template+"_"+String.valueOf(from)+"_"+String.valueOf(to)+".pdb";
    }

    public String getCoordinates() {
        return coordinates;
    }

    public void map() throws Exception {

        Structure structure = StructureUtils.getModelStructureLocal(coordinates);

        Chain chain = structure.getChainByIndex(0);
        List<Group> groups = chain.getAtomGroups();

        String uniSeq = alignment.split("\n")[1].replaceAll("-", "");
        ProteinSequence uniprotSequence = new ProteinSequence(uniSeq);

        String modSeq = chain.getAtomSequence();
        ProteinSequence modelSequence = new ProteinSequence(modSeq);

        IsoformMapper isomapper = new IsoformMapper(uniprotSequence, modelSequence);

        uniprotCoordinates = IntStream.range(from, to+1).toArray();
        modelCoordinates = new int[uniprotCoordinates.length];

        int tInd = 0;
        for ( int i=0; i < uniprotCoordinates.length; i++ ) {

            int pos = isomapper.convertPos1toPos2(i + 1);
            if ( pos == -1 ) {
                modelCoordinates[i] = -1;
            }
            else {
                modelCoordinates[i] = groups.get(tInd).getResidueNumber().getSeqNum();
                tInd++;
            }
        }
    }

    public int getModelCoordinateByUniprotPosition(int pos) {

        int ind = Ints.indexOf(uniprotCoordinates, pos);
        if (ind == -1)
            return ind;
        return modelCoordinates[ind];
    }

    public int getFirstResidueNumber() {

        for (int i=0; i<modelCoordinates.length;i++) {
            if ( modelCoordinates[i] != -1 )
                return modelCoordinates[i];
        }
        return -1;
    }

    public int getLastResidueNumber() {

        for ( int i=modelCoordinates.length-1; i>=0; i-- ) {
            if ( modelCoordinates[i] != -1 )
                return modelCoordinates[i];
        }
        return -1;
    }
}
