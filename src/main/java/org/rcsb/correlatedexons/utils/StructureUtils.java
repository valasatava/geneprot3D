package org.rcsb.correlatedexons.utils;

import org.biojava.nbio.structure.*;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.io.LocalPDBDirectory;
import org.biojava.nbio.structure.io.PDBFileReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yana on 4/20/17.
 */
public class StructureUtils {

    public static Structure getBioJavaStructure(String pdbId) throws IOException, StructureException {

        AtomCache cache = new AtomCache();
        cache.setUseMmCif(true);

        StructureIO.setAtomCache(cache);

        Structure structure = null;
        try {
            structure = StructureIO.getStructure(pdbId);
        } catch (FileNotFoundException e1) {
            cache.setObsoleteBehavior(LocalPDBDirectory.ObsoleteBehavior.FETCH_OBSOLETE);
            structure = StructureIO.getStructure(pdbId);
        }
        return structure;
    }

    public static Structure getModelStructure(String modelUrl) throws Exception {

        URL url = new URL(modelUrl);

        PDBFileReader reader = new PDBFileReader();
        Structure structure = reader.getStructure(url);

        return structure;
    }

    public static double getMinDistance(List<Atom> a1, List<Atom> a2) {

        double min = 999.0d;
        for (Atom aa1 : a1) {
            for (Atom aa2 : a2) {
                double distance = Calc.getDistance(aa1, aa2);
                if (distance < min) {
//                    if ( distance == 0.0 )
//                        System.out.println();
                    min = distance;
                }
            }
        }
        return min;
    }

    public static List<Group> getGroupsInRange(List<Group> groups, int start, int end) {

        List<Group> range = new ArrayList<Group>();

        if ( start!=-1 && end != -1 ) {
            range = groups.stream().filter(g -> ((g.getResidueNumber().getSeqNum() >= start) && (g.getResidueNumber().getSeqNum() <= end))).collect(Collectors.toList());
        }

        else if (end == -1) {

            List<Group> tail = groups.stream().filter(g -> (g.getResidueNumber().getSeqNum() >= start)).collect(Collectors.toList());
            if (tail.size()<=1)
                return tail;

            int ind=1;
            range.add(tail.get(0));
            while ( ( tail.get(ind).getResidueNumber().getSeqNum() - tail.get(ind-1).getResidueNumber().getSeqNum() ) == 1 ) {
                range.add(tail.get(ind));
                ind++;
                if ( ind+1>=tail.size()) {
                    range.add(tail.get(ind));
                    break;
                }
            }
        }

        else if (start == -1) {

            List<Group> head = groups.stream().filter(g -> (g.getResidueNumber().getSeqNum() <= end)).collect(Collectors.toList());

            if (head.size()<=1)
                return head;

            int ind=head.size()-1;
            range.add(head.get(head.size()-1));
                while ( ( head.get(ind-1).getResidueNumber().getSeqNum() - head.get(ind).getResidueNumber().getSeqNum() ) == 1 ) {
                    range.add(head.get(ind));
                    ind--;
                    if ( ind-1<0 ) {
                        range.add(head.get(ind));
                        break;
                    }
                }
        }
        return range;
    }

    public static List<Atom> getAtomsInRange(List<Group> groups, int start, int end) {

        List<Group> g = getGroupsInRange(groups, start, end);

        List<Atom> atoms = new ArrayList<Atom>();
        for (Group group : g) {
            List<Atom> a = group.getAtoms();
            atoms.addAll(a);
        }
        return atoms;
    }
}
