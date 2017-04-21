package org.rcsb.genevariation.mappers;

import org.apache.commons.lang3.StringUtils;
import org.biojava.nbio.alignment.Alignments;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceAligner;
import org.biojava.nbio.core.alignment.matrices.SubstitutionMatrixHelper;
import org.biojava.nbio.core.alignment.template.SequencePair;
import org.biojava.nbio.core.alignment.template.SubstitutionMatrix;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.junit.Test;
import org.rcsb.correlatedexons.utils.StructureUtils;
import org.rcsb.uniprot.isoform.IsoformMapper;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yana on 4/21/17.
 */
public class TestUniprotToHomologyMapper {

    @Test
    public void testMapping() throws Exception {

        //SwissModel API: https://swissmodel.expasy.org/repository/uniprot/O94856.json?provider=swissmodel

        String[] alignment = {"TQPPTITKQSAKDHIVDPRDNILIECEAKGNPAPSFHWTR-NSRFFNIAKDPRVSMRRRSGTLVIDFRSG--GRPEEYEGEYQCFARNKFGTALSNRIRLQVSKSPLWPKENLDPVVVQEGAPLTLQCNPPPGLPS--PVIFWMSSSMEPIT----QDKRVSQGHNGDLYFSNVMLQDM-QTDYSCNARFHFTHTIQQK-NPFTLKVLTTRGVAERTPSFMYPQGTASSQMVLRGMDLLLECIASGVPTPDIAWYKKGGDLPSDK-----AKFENFNKALRITNVSEEDSGEYFCLASNKMGSIRHTISVRVKAAPYWLDEPKNLILAPGEDGRLVCRANGNPKPTVQWMVNGEPLQSAPPNPNREVAGDTIIFRDTQISSRAVYQCNTSNEHGYLLANAFVSVL--DVPPRMLSPRNQLIRVILYNRTRLDCPFFGSPIPTLRWFKNGQGSNLDGGNYHV----YENG----SLEIKMIRKEDQGIYTCVATNILGKAENQVRLEVKDPTRIYRMPEDQVARRGTTVQLECRVKHDPSLKLTVSWLKDDEPLYIGNRMKK-EDDSLTIFGVA-ERDQGSYTCVASTELD-QDLAKAYLTVLADQATPTNRLAALPKGRPDRPRDLELTDLAERSVRLTWIPGDANNSPITDYVVQFEEDQF--QPGVWHDHS-KYPGSVNSAVLRLSPYVNYQFRVIAINEVGSSHPSLPSERYRTSGAPPESNPGDVKGEGTRK----------NNMEITWTPMNATSAFGPNLRYIVKWRRRETREAWNNVTVWGSRYVVGQTPVYVPYEIRVQAENDFGKGPE",
                              "QKGPVFLKEPTNRIDFSNSTGAEIECKASGNPMPEIIWIRSDGTAVGDV--PGLRQISSDGKLVFPPFRAEDYRQEVHAQVYACLARNQFGSIISRDVHVRAVVAQYYE-ADVNKEHVIRGNSAVIKCLIPSFVADFVEVVSWHTDEEENYFPGAEYDGKYLVLPSGELHIREVGPEDGYKS-YQCRTKHRLTGETRLSATKGRLVITE--PISSAVPKVVSL-AKFDMKTYSGSSTMALLCPAQGYPVPVFRWYKFIEGTTRKQAVVLNDRVKQVSGTLIIKDAVVEDSGKYLCVVNNSVGGESVETVLTVTAPLSAKIDPPTQTVDFGRPAVFTCQYTGNPIKTVSWMKDGKAIGH---------SESVLRIESVKKEDKGMYQCFVRNDRESAEASAELKLGGRFDPPVIRQAFQ-EETMEPGPSVFLKCVAGGNPTPEISWELDGKKIAN-NDRYQVGQYVTVNGDVVSYLNITSVHANDGGLYKCIAKSKVGVAEHSAKLNVYGLPYI-RQMEKKAIVAGETLIVTCPVAGYPI--DSIVWERDNRALPINRKQKVFPNGTLIIENVERNSDQATYTCVAKNQEGYSARGSLEVQVMVLPRIIP-----------FAFEEG--PAQVGQYLTLHCSVPGG-DLP---LNIDWTLDGQAISEDLGITTSRVGRRGSVLTIEAVEASHAGNFTCHARNLAGHQQFTTPLNV-YVPPR-WILEPTDKAFAQGSDAKVECKADGFPKPQVTWKKAV-----GDTPGEYKDLKK---S---DNIRVEEGTLHVDNIQKTNEGYYLCEAINGIGSGLS"};

        Structure structure = StructureUtils.getModelStructure("https://swissmodel.expasy.org/repository/uniprot/O94856.pdb?range=38-812&template=3dmk.1.A&provider=swissmodel");
        Chain chain = structure.getChainByIndex(0);
        List<Group> groups = chain.getAtomGroups();

        String proteinSeq = alignment[0].replaceAll("-","");
        ProteinSequence s1 = new ProteinSequence(proteinSeq);

        String templateSeq = chain.getAtomSequence();
        ProteinSequence s2 = new ProteinSequence(templateSeq);

        IsoformMapper isomapper = new IsoformMapper(s1, s2);
        System.out.println(isomapper.getPair().toString(100));

        int pos21 = isomapper.convertPos1toPos2(1);
        int pos22 = isomapper.convertPos1toPos2(2);
        
        int from = 38; int start = 626; int end = 660; int to = 812;

        String[] proteinSequence = alignment[0].split("");
        String[] templateSequence = alignment[1].split("");

        int[] uniprotCoordinates = new int[proteinSequence.length];
        int[] modelCoordinates = new int[templateSequence.length];

        int qInd = 0;
        int tInd = 0;

        for (int i=0; i < proteinSequence.length; i++) {

            String aa = proteinSequence[i];
            String ma = templateSequence[i];

            if ( !aa.equals("-") ) {

                if ( !ma.equals("-") ) {

                    uniprotCoordinates[i] = from+qInd;
                    modelCoordinates[i] = groups.get(tInd).getResidueNumber().getSeqNum();
                    qInd ++;
                    tInd ++;

                    if (tInd==574) {
                        System.out.println();
                    }

                }
                else {
                    uniprotCoordinates[i] = from+qInd;
                    modelCoordinates[i] = -1;
                    qInd++;
                }
            }
            else {
                uniprotCoordinates[i] = -1;
                modelCoordinates[i] = groups.get(tInd).getResidueNumber().getSeqNum();
                tInd++;
                if (tInd==574) {
                    System.out.println();
                }
            }
        }
        System.out.println();
    }

}
