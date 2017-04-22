package org.rcsb.genevariation.mappers;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by yana on 4/21/17.
 */
public class TestUniprotToHomologyMapper {

    @Test
    public void testMapping() throws Exception {

        //SwissModel API: https://swissmodel.expasy.org/repository/uniprot/O94856.json?provider=swissmodel

        UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper("O94856");
        mapper.setTemplate("https://swissmodel.expasy.org/repository/uniprot/O94856.pdb?range=38-812&template=3dmk.1.A&provider=swissmodel");
        mapper.map();

        assertEquals(-1, mapper.getModelCoordinateByUniprotPosition(38));
        assertEquals(39, mapper.getModelCoordinateByUniprotPosition(39));
    }
}
