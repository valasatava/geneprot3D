package org.rcsb.genevariation.mappers;

import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.correlatedexons.utils.StructureUtils;
import org.rcsb.genevariation.utils.CommonUtils;
import org.rcsb.uniprot.isoform.IsoformMapper;
import org.spark_project.guava.primitives.Ints;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by yana on 4/21/17.
 */
public class UniprotToModelCoordinatesMapper {

    private String uniprotId;

    private int from;
    private int to;

    private String alignment;
    private String coordinates;

    private int[] uniprotCoordinates;
    private int[] modelCoordinates;

    public UniprotToModelCoordinatesMapper(String uniprotId) {
        
        this.uniprotId = uniprotId;
    }

    public void setTemplate(String template) throws Exception {

        JSONArray homologyArray = CommonUtils.readJsonArrayFromUrl("https://swissmodel.expasy.org/repository/uniprot/" + uniprotId + ".json?provider=swissmodel");

        for (int i = 0; i < homologyArray.length(); i++) {

            JSONObject homologyObject = homologyArray.getJSONObject(i);

            if ( ! homologyObject.getString("coordinates").equals(template))
                continue;

            from = homologyObject.getInt("from");
            to = homologyObject.getInt("to");

            alignment = homologyObject.getString("alignment");
            coordinates = homologyObject.getString("coordinates");

            break;
        }
    }

    public void map() throws Exception {

        Structure structure = StructureUtils.getModelStructure(coordinates);

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
}
