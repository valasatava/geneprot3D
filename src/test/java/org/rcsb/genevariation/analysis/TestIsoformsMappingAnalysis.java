package org.rcsb.genevariation.analysis;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by yana on 3/28/17.
 */
public class TestIsoformsMappingAnalysis {

    @Test
    public void testSameLength() throws CompoundNotFoundException {

        ProteinSequence[] sequencesSame = new ProteinSequence[3];
        sequencesSame[0] = new ProteinSequence("STKVLVELYAMTVFMSANCKYACICEILVMRSKSSSP");
        sequencesSame[1] = new ProteinSequence("STKVLVELYAMTVFMSANCKYACICEILVMRSKSSSP");
        sequencesSame[2] = new ProteinSequence("STKVLVELYAMTVFMSANCKYACICEIL");

        assertTrue(IsoformsMappingAnalysis.sameLength(sequencesSame));

        ProteinSequence[] sequencesDiff = new ProteinSequence[3];
        sequencesDiff[0] = new ProteinSequence("STKVLVELYAMTVFMSANCKYACICEI");
        sequencesDiff[1] = new ProteinSequence("STKVLVELYAMTVFMSANCKYACICEILVMRSKSSSP");
        sequencesDiff[2] = new ProteinSequence("STKVLVELYAMTVFMS");

        assertFalse(IsoformsMappingAnalysis.sameLength(sequencesDiff));

    }
}
