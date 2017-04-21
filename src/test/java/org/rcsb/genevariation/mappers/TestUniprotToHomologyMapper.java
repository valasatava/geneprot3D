package org.rcsb.genevariation.mappers;

import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Structure;
import org.junit.Test;
import org.rcsb.correlatedexons.utils.StructureUtils;

/**
 * Created by yana on 4/21/17.
 */
public class TestUniprotToHomologyMapper {

    @Test
    public void testMapping() throws Exception {

        String[] alignment = {"PQITLWQRPLVTIKIGGQLKEALLDTGADDTVLEEMSLPGRWKPKMIGGIGGFIKVRQYDQILIEICGHKAIGTVLVGPTPVNIIGRNLLTQIGCTLNFPISPIETVPVKLKPGMDGPKVKQWPLTEEKIKALVEICTEMEKEGKISKIGPENPYNTPVFAIKKKDSTKWRKLVDFRELNKRTQDFWEVQLGI-PHPAGLKKKKSVTVLDVGDAYFSVPLDEDFRKY",
                              "PQITLWKRPLVTIRIGGQLKEALLNTGSDFTVLEEMNLPGKWKPKMIRGIGGFIKVRQYDQIPVEILGHKAIGTVLVGPTPVNIIGRNLLTQIGMTLNFGGS---------------------------------------------SGPQITLWKRPLVTIRIG-GQLKEALLNTGSDDTVLEEM---NLPGKWKPKMIGGIGGF--IKVRQ-YDQIPVEILGHKA"};

        Structure structure = StructureUtils.getModelStructure("https://swissmodel.expasy.org/repository/uniprot/P03366.pdb?range=501-726&template=4eqj.1.A&provider=swissmodel");
        Chain chain = structure.getChainByIndex(0);

        System.out.println();
    }

}
