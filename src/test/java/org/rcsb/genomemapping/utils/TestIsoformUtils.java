package org.rcsb.genomemapping.utils;

import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.junit.Test;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.genomemapping.utils.IndexOffset;
import org.rcsb.geneprot.genomemapping.utils.IsoformUtils;

import javax.xml.bind.JAXBException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Yana Valasatava on 10/23/17.
 */
public class TestIsoformUtils {

    @Test
    public void testReplace() {

        String original = "MEPRSMEYFCAQVQ";
        String variation = "RPFSSKW";

        String test = "MRPFSSKWSMEYFCAQVQ";
        String result = IsoformUtils.replace(original, 2, 4, variation, new IndexOffset(original.length()));

        assertEquals(test, result);

    }

    /** This unit test covers a case when getting isoforms for O75122 UniProt entry gives "The extracted sequence does not match
     * the info in UniProt!" error which results in braking the Protein Feature View. This raises in @see org.rcsb.action.explore.EntityViewAction class
     * giving the "Error when recreating isoform (replacement of variant) from UniProt for O75122-3" error.
     *
     * @throws IOException
     */
    @Test
    public void testBuildIsoform3ForO75122() throws IOException {

        String uniProtId = "O75122";

        String canonical = ExternalDBUtils.getCanonicalUniProtSequence(uniProtId)
                .collectAsList().get(0).getString(0);

        String refs = "VSP_057273 VSP_057274 VSP_057275 VSP_057276";

        String isoformSequenceResult =  IsoformUtils.buildIsoform(canonical, refs);

        String isoformSequenceTest = "MEPRSMEYFCAQVQQKDVGGRLQVGQELLLYLGAPGAISDLEEDLGRLGKTVDALTGWVG" +
                "SSNYRVSLMGLEILSAFVDRLSTRFKSYVAMVIVALIDRMGDAKDKVRDEAQTLILKLMD" +
                "QVAPPMYIWEQLASGFKHKNFRSREGVCLCLIETLNIFGAQPLVISKLIPHLCILFGDSN" +
                "SQVRDAAILAIVEIYRHVGEKVRMDLYKRGIPPARLEMIFAKFDEVQSSGGMILSVCKDK" +
                "SFDDEESVDGNRPSSAASAFKVPAPKTSGNPANSARKPGSAGGPKVGAGASKEGGAGAVD" +
                "EDDFIKAFTDVPSIQIYSSRELEETLNKIREILSDDKHDWDQRANALKKIRSLLVAGAAQ" +
                "YDCFFQHLRLLDGALKLSAKDLRSQVVREACITVAHLSTVLGNKFDHGAEAIVPTLFNLV" +
                "PNSAKVMATSGCAAIRFIIRHTHVPRLIPLITSNCTSKSVPVRRRSFEFLDLLLQEWQTH" +
                "SLERHAAVLVETIKKGIHDADAEARVEARKTYMGLRNHFPGEAETLYNSLEPSYQKSLQT" +
                "YLKSSGSVASLPQSDRSSSSSQESLNRPFSSKWSTANPSTVAGRVSAGSSKASSLPGSLQ" +
                "RSRSDIDVNAAAGAKAHHAAGQSVRRGRLGAGALNAGSYASLEDTSDKLDGTASEDGRVR" +
                "AKLSAPLAGMGNAKADSRGRSRTKMVSQSQPGSRSGSPGRVLTTTALSTVSSGVQRVLVN" +
                "SASAQKRSKIPRSQGCSREASPSRLSVARSSRIPRPSVSQGCSREASRESSRDTSPVRSF" +
                "QPLGPGYGISQSSRLSSSVSAMRVLNTGSDVEEAVADALLLGDIRTKKKPARRRYESYGM" +
                "HSDDDANSDASSACSERSYSSRNGSIPTYMRQTEDVAEVLNRCASSNWSERKEGLLGLQN" +
                "LLKNQRTLSRVELKRLCEIFTRMFADPHGKRVFSMFLETLVDFIQVHKDDLQDWLFVLLT" +
                "QLLKKMGADLLGSVQAKVQKALDVTRESFPNDLQFNILMRFTVDQTQTPSLKVKVAILKY" +
                "IETLAKQMDPGDFINSSETRLAVSRVITWTTEPKSSDVRKAAQSVLISLFELNTPEFTML" +
                "LGALPKTFQDGATKLLHNHLRNTGNGTQSSMGSPLTRPTPRSPANWSSPLTSPTNTSQNT" +
                "LSPSAFDYDTENMNSEDIYSSLRGVTEAIQNFSFRSQEDMNEPLKRDSKKDDGDSMCGGP" +
                "GMSDPRAGGDATDSSQTALDNKASLLHSMPTHSSPRSRDYNPYNYSDSISPFNKSALKEA" +
                "MFDDDADQFPDDLSLDHSDLVAELLKELSNHNERVEERKIALYELMKLTQEESFSVWDEH" +
                "FKTILLLLLETLGDKEPTIRALALKVLREILRHQPARFKNYAELTVMKTLEAHKDPHKEV" +
                "VRSAEEAASVLATSISPEQCIKVLCPIIQTADYPINLAAIKMQTKVIERVSKETLNLLLP" +
                "EIMPGLIQGYDNSESSVRKACVFCLVAVHAVIGDELKPHLSQLTGSKMKLLNLYIKRAQT" +
                "GSGGADPTTDVSGQS";

        assertEquals(isoformSequenceTest, isoformSequenceResult);
    }

    @Test
    public void testBuildIsoform2ForQ5JS13() throws JAXBException, CompoundNotFoundException, IOException {

        String uniProtId = "Q5JS13";
        String canonical = ExternalDBUtils.getCanonicalUniProtSequence(uniProtId)
                .collectAsList().get(0).getString(0);

        String refs = "VSP_033478 VSP_033480 VSP_033481";

        String isoformSequenceResult = IsoformUtils.buildIsoform(canonical, refs);

        String isoformSequenceTest = "MYKRNGLMASVLVTSATPQGSSSSDSLEGQSCDYASKSYDAVVFDVLKVTPEEFASQITL" +
                "MDIPVFKAIQPEELASCGWSKKEKHSLAPNVVAFTRRFNQVSFWVVREILTAQTLKIRAE" +
                "ILSHFVKIAKKLLELNNLHSLMSVVSALQSAPIFRLTKTWALLNRKDKTTFEKLDYLMSK" +
                "EDNYKRTREYIRSLKMVPSIPYLGIYLLDLIYIDSAYPASGSIMENEQRSNQMNNILRII" +
                "ADLQVSCSYDHLTTLPHVQKYLKSVRYIEELQKFVEDDNYKLSLRIEPGSSSPRLVSSKE" +
                "DLAGPSAGSGSARFSRRPTCPDTSVAGSLPTPPVPRHRKSHSLGNNRGRLYATLGPNWRV" +
                "PVRNSPRTRSCVYSPTGPCICSLGNSAAVPTMEGPLRRKTLLKEGRKPALSSWTRYWVIL" +
                "SGSTLLYYGAKSLRGTDRKHVSIVGWMVQLPDDPEHPDIFQLNNPDKGNVYKFQTGSRFH" +
                "AILWHKHLDDACKSNRPQEAGAAPGPTGTDSHEVDHLEGGAGKEAGPCA";

        assertEquals(isoformSequenceTest, isoformSequenceResult);
    }

    @Test
    public void testBuildIsoform3ForQ5JS13() throws JAXBException, CompoundNotFoundException, IOException {

        String uniProtId = "Q5JS13";
        String canonical = ExternalDBUtils.getCanonicalUniProtSequence(uniProtId)
                .collectAsList().get(0).getString(0);

        String refs = "VSP_033475 VSP_033479";

        String isoformSequenceResult = IsoformUtils.buildIsoform(canonical, refs);

        String isoformSequenceTest = "MYKRNGLMASVLVTSATPQGSSSSDSLEGQSCDYASKSYDAVVFDVLKVTPEEFASQITL" +
                "MDIPVFKAIQPEELASCGWSKKEKHSLAPNVVAFTRRFNQVSFWVVREILTAQTLKIRAE" +
                "ILSHFVKIAKKLLELNNLHSLMSVVSALQSAPIFRLTKTWALLNRKDKTTFEKLDYLMSK" +
                "EDNYKRTREYIRSLKMVPSIPYLGIYLLDLIYIDSAYPASGSIMENEQRSNQMNNILRII" +
                "ADLQVSCSYDHLTTLPHVQKYLKSVRYIEELQKFVEDDNYKLSLRIEPGSSSPRLVSSKE" +
                "DLAVSHLSSLSHQGQAEEARLKPTSGQHPAWMWPSSSRVPAAPPASAAPRSPWPRNLRND" +
                "QGQPGAVALTCNPSTLGSRSRRIT";

        assertEquals(isoformSequenceTest, isoformSequenceResult);
    }

    @Test
    public void testBuildIsoform4ForQ5JS13() throws JAXBException, CompoundNotFoundException, IOException {

        String uniProtId = "Q5JS13";
        String canonical = ExternalDBUtils.getCanonicalUniProtSequence(uniProtId)
                .collectAsList().get(0).getString(0);

        String refs = "VSP_033476 VSP_033477";

        String isoformSequenceResult = IsoformUtils.buildIsoform(canonical, refs);

        String isoformSequenceTest = "MYKRNGLMASVLVTSATPQGSSSSDSLEGQSCDYASKSYDAVVFDVLKVTPEEFASQITL" +
                "MDIPVFKAIQPEELASCGWSKKEKHSLAPNVVAFTRRFNQVSFWVVREILTAQTLKIRAE" +
                "ILSHFVKIAKKLLELNNLHSLMSVVSALQSAPIFRLTKTWALLNRKDKTTFEKLDYLMSK" +
                "EDNYKRTREYIRSLKMVPSIPYLGIYLLDLIYIDSAYPASGSIMENEQRSNQMNNILRII" +
                "ADLQVSCSYDHLTTLPHVQKYLKSVRYIEELQKFVEDDNYKLSLRIEPGSSSPRLVSSKE" +
                "DLAAM";

        assertEquals(isoformSequenceTest, isoformSequenceResult);
    }
}
