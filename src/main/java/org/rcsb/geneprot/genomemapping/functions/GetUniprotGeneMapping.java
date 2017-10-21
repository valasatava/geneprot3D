package org.rcsb.geneprot.genomemapping.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.util.SequenceTools;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.biojava.nbio.genome.util.ProteinMappingTools;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonUtils;
import org.rcsb.geneprot.gencode.dao.MetadataDAO;
import org.rcsb.geneprot.genomemapping.utils.ChromosomeUtils;
import org.rcsb.humangenome.function.SparkGeneChromosomePosition;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.auto.dao.UniprotDAO;
import org.rcsb.uniprot.auto.dao.UniprotDAOImpl;
import org.rcsb.uniprot.auto.tools.JpaUtilsUniProt;
import org.rcsb.uniprot.isoform.IsoformMapper;
import org.rcsb.uniprot.isoform.IsoformTools;
import org.rcsb.util.Parameters;
import org.rcsb.util.UniprotGeneMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.persistence.EntityManager;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 */
public class GetUniprotGeneMapping implements PairFlatMapFunction<Tuple2<String, SparkGeneChromosomePosition>, String,UniprotGeneMapping> {

    private static final Logger PdbLogger = LoggerFactory.getLogger(GetUniprotGeneMapping.class);

    private static Map<String, String> genes;
    private static Map<String, String> uniprotIds;
    private static UniprotDAO uniprotDAO;

    public GetUniprotGeneMapping()  throws Exception
    {
        try {
            uniprotDAO = new UniprotDAOImpl();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (genes == null)
            genes = MetadataDAO.getGeneSymbols();
        if (uniprotIds == null)
            uniprotIds = MetadataDAO.getGeneSwissProt();
    }

    @Override
    public Iterator<Tuple2<String, UniprotGeneMapping>> call(Tuple2<String, SparkGeneChromosomePosition> position) throws Exception
    {
        List<Tuple2<String, UniprotGeneMapping>> data = new ArrayList<>();

        String chromosome = position._1;
        SparkGeneChromosomePosition chromosomePosition = position._2;

        String geneSymbol = chromosomePosition.getGeneName();
        Set<String> keys = CommonUtils.getKeysByValue(genes, geneSymbol);
        List<String> uniprots = new ArrayList<>();
        for (String transcriptId : keys) {
            String uid = uniprotIds.get(transcriptId);
            if (uid != null && ! uniprots.contains(uid))
                uniprots.add(uid);
        }

        if (uniprots.size()==0)
            return data.iterator();

        String uniProtId = uniprots.get(0);
        if(StringUtils.isBlank(uniProtId))
            return data.iterator();

        // there is a data issue on chr2 where two IDs are merged by a comma
        int cpos = uniProtId.indexOf(",");
        if (  cpos > -1) {
            uniProtId = uniProtId.substring(0, cpos);
        }

        EntityManager upEM = JpaUtilsUniProt.getEntityManager();
        Uniprot uniProt = uniprotDAO.getUniProt(upEM, uniProtId);

        if(uniProt == null)
            return data.iterator();

        PdbLogger.info("Processing "+ chromosome.toUpperCase() + ": gene: " + chromosomePosition.getGeneName() + "; " + " UniProt Id: " + uniProtId);

        data = getGeneMappingForChromosomePosition(uniProtId, geneSymbol, uniProt, chromosomePosition);

        upEM.close();

        return data.iterator();
    }

    private List<Tuple2<String,UniprotGeneMapping>> getGeneMappingForChromosomePosition(String uniProtId, String geneSymbol,
                                                                                        Uniprot uniprot, SparkGeneChromosomePosition chromosomePosition)
    {
        List<Tuple2<String, UniprotGeneMapping>> results = new ArrayList<>();

        try {
            ChromosomeMappingTools.setCoordinateSystem(0);
            int dnaLength = ChromosomeMappingTools.getCDSLength(chromosomePosition.getOrig());
            ChromosomeUtils cu = new ChromosomeUtils();
            if (dnaLength > ChromosomeUtils.MAX_DNA_LENGTH) {
                // to prevent out of memory for the Isoform mapping, we skip too long genes
                PdbLogger.warn("Gene "+ geneSymbol+ " has  " + dnaLength + " bp. We will skip the mapping to prevent out of memory errors");
                return results;
            }

            if (dnaLength < 1) {
                PdbLogger.warn("Skipping gene "+ geneSymbol+" ("+chromosomePosition.getGenebankId()+ "). Problem with retrieving the DNA length from the chromosome position for: " + chromosomePosition.getChromosome());
                return results;
            }

            if(uniProtId == null){
                PdbLogger.warn("uniprotId is null. skipping chromosome position");
                return results;
            }

            if (StringUtils.isBlank(uniProtId)) {
                PdbLogger.warn("Skipping gene symbol "+ geneSymbol+ ". Did not find a Uniprot name for: " + geneSymbol);
                return results;
            }

            ProteinSequence[] isoforms = null;
            ProteinSequence correctIsoform = null;

            IsoformMapper isomapper = null;
            IsoformTools iso = new IsoformTools();
            try {
                isoforms = iso.getIsoforms(uniprot);
            } catch (Exception e){
                PdbLogger.error(e.getMessage());
                return results;
            }

            int isoformNr = -1;

            if (SequenceTools.equalLengthSequences(isoforms)) {

                Path twoBitFolderPath = Paths.get(Parameters.getWorkDirectory() + "/2bit/");
                if ( ! Files.exists(twoBitFolderPath)) {
                    Files.createDirectories(twoBitFolderPath);
                }
                DataLocationProvider.setGenome("mouse");
                File twoBitFileLocalLocation = new File(DataLocationProvider.getGenomeLocation());

                TwoBitFacade twoBitFacade = new TwoBitFacade(twoBitFileLocalLocation);

                DNASequence transcriptDNASequence = ChromosomeMappingTools.getTranscriptDNASequence(twoBitFacade,
                        chromosomePosition.getChromosome(), chromosomePosition.getExonStarts(), chromosomePosition.getExonEnds(),
                        chromosomePosition.getCdsStart(), chromosomePosition.getCdsEnd(), chromosomePosition.getOrientation().charAt(0));

                ProteinSequence sequence = null;
                try {
                    sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptDNASequence);
                } catch (Exception e) {
                    PdbLogger.info("Could not translate the transcript DNA sequence for "+ chromosomePosition.getGeneName()+" "+ chromosomePosition.getGenebankId());
                    return results;
                }

                isoformNr = iso.getUniprotIsoformPositionByEqualSequence(isoforms, sequence);
            }
            else {
                isoformNr = iso.getUniprotIsoformPositionByLength(isoforms, dnaLength / 3);
            }

            if (isoformNr ==  -1) {
                PdbLogger.info("Did not find a matching protein sequence of length for uniprot: " + uniProtId + "(" + uniprot.getEntry().get(0).getAccession().get(0)+ ") and dna length: " + (dnaLength / 3) + " " + geneSymbol);
                return results;
            }

            correctIsoform = isoforms[isoformNr];
            int length = correctIsoform.getLength();

            if (length * 3 != dnaLength) {
                PdbLogger.error("Mapping problem. Coding DNA sequence is not 3x the length of the protein sequence! " + uniProtId + " " + dnaLength + " " + (length * 3));
                return results;
            }

            // we map the main uniprot sequence and the isoform used in the genome mapping
            // so we can project across
            isomapper = new IsoformMapper(isoforms[0], correctIsoform);

            Map<Integer, Integer> exonMap = cu.getExonPositions(chromosomePosition.getOrig());

            int transcriptionStart = chromosomePosition.getTranscriptionStart();
            int transcriptionEnd = chromosomePosition.getTranscriptionEnd();

            if(transcriptionStart > transcriptionEnd){
                //switch them so start is < end
                int tmp = transcriptionEnd;
                transcriptionEnd = transcriptionStart;
                transcriptionStart = tmp;
            }

            int cdsStart = chromosomePosition.getCdsStart();
            int cdsEnd = chromosomePosition.getCdsEnd();

            if(cdsStart > cdsEnd){
                //switch them
                int tmp = cdsEnd;
                cdsEnd = cdsStart;
                cdsStart = tmp;
            }

            if (chromosomePosition != null && chromosomePosition.getOrientation().equals("+")) {

                int mRNAPos = 0;

                for(int absPosition = transcriptionStart; absPosition <= transcriptionEnd; absPosition++){

                    UniprotGeneMapping ugm = new UniprotGeneMapping();

                    ugm.setUniProtId(uniProtId);
                    ugm.setPosition(absPosition);
                    ugm.setChromosome(chromosomePosition.getChromosome());
                    ugm.setGeneName(""); // TODO: map gene symbols to names
                    ugm.setGeneSymbol(geneSymbol);
                    ugm.setIsoformIndex(isoformNr);

                    //set defaults
                    ugm.setInCoding(Boolean.FALSE);
                    ugm.setInUtr(Boolean.TRUE);
                    ugm.setExonNum(-1);
                    ugm.setMRNAPos(-1);
                    ugm.setUniProtIsoformPos(-1);
                    ugm.setUniProtCanonicalPos(-1);
                    ugm.setGeneBankId(chromosomePosition.getGenebankId());

                    if (chromosomePosition != null && chromosomePosition.getOrientation().equals("+")) {
                        ugm.setOrientation("+");
                    } else {
                        ugm.setOrientation("-");
                    }

                    if ( absPosition >= cdsStart && absPosition <= cdsEnd ) {

                        ugm.setInUtr(Boolean.FALSE);

                        if(exonMap.keySet().contains(absPosition)){

                            ugm.setMRNAPos(mRNAPos);
                            ugm.setUniProtIsoformPos(mRNAPos/3 + 1);
                            ugm.setExonNum(exonMap.get(absPosition));
                            ugm.setInCoding(Boolean.TRUE);

                            try {
                                ugm.setUniProtCanonicalPos(isomapper.convertPos2toPos1(ugm.getUniProtIsoformPos()));
                            } catch (Exception e){
                                PdbLogger.error("problem with resolving alignment for position: " + absPosition + " uniprot id: " + uniProtId + " alignment length:" + isomapper.getAlignmentLength() + " mRNA pos:" + mRNAPos);
                            }

                            //only increment if it is in coding region and in an exon
                            mRNAPos++;
                        }
                    }

//                    if ( ! ugm.isInCoding())
//                        continue;

                    String chromosome = ugm.getChromosome();
                    Tuple2 t2 = new Tuple2(chromosome, ugm);
                    results.add(t2);
                }

            } else {

                int mRNAPos = 0;

                for(int absPosition = transcriptionEnd; absPosition >= transcriptionStart; absPosition--){

                    UniprotGeneMapping ugm = new UniprotGeneMapping();

                    ugm.setUniProtId(uniProtId);
                    ugm.setPosition(absPosition);
                    ugm.setChromosome(chromosomePosition.getChromosome());
                    ugm.setGeneName("");
                    ugm.setGeneSymbol(geneSymbol);
                    ugm.setIsoformIndex(isoformNr);

                    //set defaults
                    ugm.setInCoding(Boolean.FALSE);
                    ugm.setInUtr(Boolean.TRUE);
                    ugm.setExonNum(-1);
                    ugm.setMRNAPos(-1);
                    ugm.setUniProtIsoformPos(-1);
                    ugm.setUniProtCanonicalPos(-1);
                    ugm.setPhase(-1);
                    ugm.setGeneBankId(chromosomePosition.getGenebankId());

                    if (chromosomePosition != null && chromosomePosition.getOrientation().equals("+")) {
                        ugm.setOrientation("+");
                    }else{
                        ugm.setOrientation("-");
                    }

                    if(absPosition >= cdsStart && absPosition <= cdsEnd){

                        if(exonMap.keySet().contains(absPosition)){
                            ugm.setMRNAPos(mRNAPos+1);
                            ugm.setPhase((mRNAPos % 3));
                            ugm.setUniProtIsoformPos(mRNAPos/3 + 1);
                            ugm.setExonNum(exonMap.get(absPosition));
                            ugm.setInUtr(Boolean.FALSE);
                            ugm.setInCoding(Boolean.TRUE);

                            try {
                                ugm.setUniProtCanonicalPos(isomapper.convertPos2toPos1(ugm.getUniProtIsoformPos()));
                            } catch (Exception e){
                                PdbLogger.error("problem with resolving alignment for position: " + absPosition + " uniprot id: " + uniProtId + " alignment length:" + isomapper.getAlignmentLength() + " mRNA pos:" + mRNAPos);
                            }

                            //only increment if it is in coding region and in an exon
                            mRNAPos++;
                        }
                    }

//                    if ( ! ugm.isInCoding())
//                        continue;

                    String chromosome = ugm.getChromosome();
                    Tuple2 t2 = new Tuple2(chromosome,ugm);
                    results.add(t2);
                }
            }

        }catch(Exception e){
            PdbLogger.error("Error creating mapping for chromosomePosition " + chromosomePosition, e.getMessage());
            PdbLogger.info("Cause: " , e);
        }

        return results;
    }
}
