package org.rcsb.correlatedexons.sandbox;

import org.biojava.nbio.structure.PDBHeader;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;

import java.io.IOException;

/**
 * Created by yana on 4/19/17.
 */
public class TestStructure {

    public static void main (String[] args) throws IOException, StructureException {

        String pdbId = "5fg8";

        AtomCache cache = new AtomCache();
        cache.setUseMmCif(true);
        StructureIO.setAtomCache(cache);

        Structure structure = StructureIO.getStructure(pdbId);
        PDBHeader header = structure.getPDBHeader();
        float resolution = header.getResolution();
        System.out.println(resolution);
    }
}
