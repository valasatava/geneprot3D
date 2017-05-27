package org.rcsb.geneprot.common.tools;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.structure.*;
import org.rcsb.geneprot.transcriptomics.utils.RowUtils;
import org.rcsb.geneprot.common.utils.StructureUtils;
import org.rcsb.geneprot.common.datastructures.PeptideRange;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.mappers.UniprotToModelCoordinatesMapper;
import org.rcsb.uniprot.auto.Entry;
import org.rcsb.uniprot.auto.FeatureType;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.config.RCSBUniProtMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yana on 5/17/17.
 */
public class PeptideRangeTool {

    private static final Logger logger = LoggerFactory.getLogger(PeptideRangeTool.class);

    public static Tuple2<PeptideRange, PeptideRange> getBestPair(List<PeptideRange> mapping1,
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

        try { Structure structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(row));

            pp.setStructureId(RowUtils.getPdbId(row) + "_" + RowUtils.getChainId(row));

            float resolution = structure.getPDBHeader().getResolution();
            pp.setResolution(resolution);

            Chain chain = structure.getPolyChainByPDB(RowUtils.getChainId(row));
            List<Group> groups = chain.getAtomGroups();

            Range<Integer> range = RowUtils.getPdbRange(row);
            List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups,
                    range.lowerEndpoint(), range.upperEndpoint());

            pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
            pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
            pp.setStructure(groupsInRange);

            return pp; }

        catch (IOException ioe) {
            logger.error("Cannot download structure: " + RowUtils.getPdbId(row));
            return pp; }

        catch (StructureException stre) {
            logger.error(stre.getMessage()+ " for: " + RowUtils.getPdbId(row));
            return pp; }
    }

    public static PeptideRange setTemplateFromRow(UniprotToModelCoordinatesMapper mapper, PeptideRange pp, Row row) {

        try { RowUtils.setUTMmapperFromRow(mapper, row);
            pp.setStructureId(RowUtils.getTemplate(row)+"_"+RowUtils.getModelFrom(row)+"_"+RowUtils.getModelTo(row));

            Range<Integer> range = RowUtils.getModelRange(mapper, row);
            List<Group> groups = StructureUtils.getGroupsFromModel(mapper.getCoordinates());
            List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(), range.upperEndpoint());
            if (groupsInRange.size() != 0) {
                pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
                pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
                pp.setStructure(groupsInRange);
            }
            return pp;
        }

        catch (FileNotFoundException fnfe) {
            logger.error("Cannot find coordinates: " + mapper.getCoordinates());
            return pp; }

        catch (CompoundNotFoundException cnfe){
            logger.error("Is not protein sequence: " + mapper.getAlignment());
            return pp; }

        catch (Exception e) {
            logger.error("Problem with: " + mapper.getCoordinates());
            return pp; }
    }

    public static PeptideRange annotateResidues(PeptideRange pr, Uniprot up, String featureType) {

        Range<Integer> exonRange = Range.closed(pr.getUniProtCoordsStart(), pr.getUniProtCoordsEnd());

        for(Entry e : up.getEntry()) {

            for (FeatureType ft : e.getFeature()) {

                if (ft.getLocation() != null && ft.getLocation().getPosition() != null &&
                        ft.getLocation().getPosition().getPosition() != null) {

                    if (ft.getType().equals(featureType)) {

                        if (ft.getLocation() != null && ft.getLocation().getPosition() != null &&
                                ft.getLocation().getPosition().getPosition() != null) {

                            if (exonRange.contains(ft.getLocation().getPosition().getPosition().intValue())) {
                                pr.addActiveSiteResidue(ft.getLocation().getPosition().getPosition().intValue());
                            }
                        }
                    }
                }
            }
        }
        return pr;
    }

    public static Map getModelForPeptidePair(Tuple2<PeptideRange, PeptideRange> pair) {

        Map model = new HashMap();

        if (pair._1.isExperimental()) { model.put("source", "rcsb://"+pair._1.getPdbId()+".mmtf"); }
        else { model.put("source", DataLocationProvider.getHumanHomologyCoordinatesLocation()
                +pair._1.getStructureId()+pair+".pdb"); }

        model.put("chain", pair._1.getChainId());
        model.put("start1", pair._1.getStructuralCoordsStart());
        model.put("end1", pair._1.getStructuralCoordsEnd());
        model.put("start2", pair._2.getStructuralCoordsStart());
        model.put("end2", pair._2.getStructuralCoordsEnd());
        model.put("distRes1", pair._1.getOtherResidues().get(0));
        model.put("distRes2", pair._2.getOtherResidues().get(0));

        String activeSites = "";
        List<Integer> sites = new ArrayList<>();
        sites.addAll(pair._1.getActiveSiteResidues());
        sites.addAll(pair._2.getActiveSiteResidues());
        if ( sites.size()!= 0) {
            activeSites = String.valueOf(sites.get(0))+":"+pair._1.getChainId();
            for ( int i=1; i<sites.size(); i++ ) {
                activeSites += " or "+ String.valueOf(sites.get(0))+":"+pair._1.getChainId();
            }
        }
        model.put("activeSites", activeSites);

        return model;
    }
}
