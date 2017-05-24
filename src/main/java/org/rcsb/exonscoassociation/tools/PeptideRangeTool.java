package org.rcsb.exonscoassociation.tools;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.structure.*;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.genevariation.datastructures.PeptideRange;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by yana on 5/17/17.
 */
public class PeptideRangeTool {

    private static final Logger logger = LoggerFactory.getLogger(PeptideRangeTool.class);

    public static Tuple2<PeptideRange, PeptideRange> getExonsPairWithBestStructuralCoverage(List<PeptideRange> mapping1,
                                                                                            List<PeptideRange> mapping2) {

        Set<String> s1 = mapping1.stream().map(t -> t.getStructureId()).collect(Collectors.toSet());
        Set<String> s2 = mapping2.stream().map(t -> t.getStructureId()).collect(Collectors.toSet());
        Sets.SetView<String> intersection = Sets.intersection(s1, s2);

        int coverage=0;
        Tuple2<PeptideRange, PeptideRange> pair = null;
        for (String structureId : intersection) {

            PeptideRange r1 = mapping1.stream().filter(t -> t.getStructureId().equals(structureId))
                    .collect(Collectors.toList()).get(0);
            PeptideRange r2 = mapping2.stream().filter(t -> t.getStructureId().equals(structureId))
                    .collect(Collectors.toList()).get(0);

            if ( (r1.getStructureLength()+r2.getStructureLength()) > coverage ) {
                pair = new Tuple2<>(r1, r2);
                coverage = r1.getStructureLength()+r2.getStructureLength();
            }
            else if ( (r1.getStructureLength()+r2.getStructureLength()) == coverage ) {
                if ( r1.isExperimental() ) {
                    if (r1.getResolution() < pair._1.getResolution()) {
                        pair = new Tuple2<>(r1, r2);
                    }
                }
            }
        }
        return pair;
    }

    public static Tuple2<Atom, Atom>  getClosestBackboneAtoms(PeptideRange p1, PeptideRange p2){

        List<Atom> backbone1 = p1.getStructure().stream()
                .map(t -> t.getAtoms().stream().filter(a -> a.getName().equals("CA")).collect(Collectors.toList()).get(0))
                .collect(Collectors.toList());

        List<Atom> backbone2 = p2.getStructure().stream()
                .map(t -> t.getAtoms().stream().filter(a -> a.getName().equals("CA")).collect(Collectors.toList()).get(0))
                .collect(Collectors.toList());

        Tuple2<Atom, Atom> atoms = StructureUtils.getAtomsAtMinDistance(backbone1, backbone2);
        return atoms;
    }

    public static Tuple2<Atom,Atom> getClosestAtoms(PeptideRange p1, PeptideRange p2) {

        List<Atom> atoms1 = p1.getStructure().stream()
                .flatMap(t -> t.getAtoms().stream())
                .collect(Collectors.toList());

        List<Atom> atoms2 = p2.getStructure().stream()
                .flatMap(t -> t.getAtoms().stream())
                .collect(Collectors.toList());

        Tuple2<Atom, Atom> atoms = StructureUtils.getAtomsAtMinDistance(atoms1, atoms2);
        return atoms;
    }

    public static PeptideRange setStructureFromRow(PeptideRange pp, Row row) {

        pp.setStructureId(RowUtils.getPdbId(row) + "_" + RowUtils.getChainId(row));
        Range<Integer> range = RowUtils.getPdbRange(row);

        Structure structure = null;
        try {structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(row)); }

        catch (IOException ioe) {
            logger.error("Cannot download structure: " + RowUtils.getPdbId(row));
            return pp; }

        catch (StructureException stre) {
            logger.error("StructureException for: " + RowUtils.getPdbId(row));
            return pp; }

        float resolution = structure.getPDBHeader().getResolution();
        pp.setResolution(resolution);

        Chain chain = structure.getPolyChainByPDB(RowUtils.getChainId(row));
        List<Group> groups = chain.getAtomGroups();
        List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(),
                range.upperEndpoint());

        pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
        pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
        pp.setStructure(groupsInRange);

        return pp;
    }

    public static PeptideRange setTemplateFromRow(UniprotToModelCoordinatesMapper mapper, PeptideRange pp, Row row) {

        pp.setStructureId(RowUtils.getTemplate(row)+".pdb");

        try { RowUtils.setUTMmapperFromRow(mapper, row); }

        catch (FileNotFoundException fnfe) {
            logger.error("Cannot find coordinates: " + mapper.getCoordinates());
            return pp; }

        catch (CompoundNotFoundException cnfe){
            logger.error("Is not protein sequence: " + mapper.getAlignment());
            return pp; }

        catch (Exception e) {
            logger.error("Problem with: " + mapper.getCoordinates());
            return pp; }

        Range<Integer> range = RowUtils.getModelRange(mapper, row);
        List<Group> groups;
        try { groups = StructureUtils.getGroupsFromModel(mapper.getCoordinates()); }
        catch (FileNotFoundException fnfe) {
            logger.error("Cannot find coordinates: " + mapper.getCoordinates());
            return pp; }
        catch (Exception e) {
            logger.error("Problem with: " + mapper.getCoordinates());
            return pp; }

        List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(), range.upperEndpoint());
        if (groupsInRange.size() != 0) {
            pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
            pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
            pp.setStructure(groupsInRange);
        }
        return pp;
    }

    public static Map getModelForPeptidePair(Tuple2<PeptideRange, PeptideRange> pair) {

        Map model = new HashMap();
        if (pair._1.isExperimental()) { model.put("source", "rcsb://"+pair._1.getPdbId()+".mmtf"); }
        else { model.put("source", DataLocationProvider.getHumanHomologyCoordinatesLocation()+"/"+pair._1.getStructureId()); }
        model.put("chain", pair._1.getChainId());
        model.put("start1", pair._1.getStructuralCoordsStart());
        model.put("end1", pair._1.getStructuralCoordsEnd());
        model.put("start2", pair._2.getStructuralCoordsStart());
        model.put("end2", pair._2.getStructuralCoordsEnd());

        return model;
    }

}
