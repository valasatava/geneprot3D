package org.rcsb.geneprot.transcriptomics.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.util.InputStreamProvider;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.biojava.nbio.genome.util.ProteinMappingTools;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.auto.tools.UniProtTools;
import org.rcsb.uniprot.isoform.IsoformTools;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by yana on 3/27/17.
 */
public class IsoformsMappingAnalysis {

    private static final String DEFAULT_MAPPING_URL="http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";

    public static boolean sameLength(ProteinSequence[] sequences) {

        for (int i=0; i<sequences.length-1; i++) {
            if (sequences[i]==null)
                continue;
            for (int j=i+1; j<sequences.length; j++) {
                if (sequences[j]==null)
                    continue;
                if (sequences[i].getLength() == sequences[j].getLength())
                    return true;
            }
        }
        return false;
    }

    public static void run() throws Exception {

        File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
        TwoBitFacade twoBitFacade = new TwoBitFacade(f);

        URL url = new URL(DEFAULT_MAPPING_URL);
        InputStreamProvider prov = new InputStreamProvider();
        InputStream inStreamGenes = prov.getInputStream(url);
        List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings(inStreamGenes);

        List<String> lines = Files.readAllLines(Paths.get("/Users/yana/data/genevariation/isoforms_all.txt"));

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr:chromosomes) {

            Dataset<Row> mapping = SparkUtils.getSparkSession().read().parquet(DataLocationProvider.getHumanGenomeMappingPath().toString()+"/"+chr);
            Dataset<Row> uniprotIds = mapping.select(mapping.col("geneSymbol"), mapping.col("uniProtId")).distinct();

            List<Row> data = uniprotIds.collectAsList();
            for (Row row :data) {

                String geneSymbol = row.getString(0);
                String uniProtId = row.getString(1);

                if (lines.contains(uniProtId))
                    continue;

                if (uniProtId.contains("Q5R372"))
                    continue;

                if (uniProtId.contains(",")) {
                    System.out.println(uniProtId);
                    String t = uniProtId.split(",")[0];
                    uniProtId = t;
                }

                URL u = UniProtTools.getURLforXML(uniProtId);
                InputStream inStream = u.openStream();
                Uniprot uniprot = UniProtTools.readUniProtFromInputStream(inStream);

                IsoformTools tools = new IsoformTools();

                ProteinSequence[] isoforms = tools.getIsoforms(uniprot);
                if ( isoforms.length <= 1 )
                    continue;

                if (sameLength(isoforms)){
                    PrintWriter pw2 = new PrintWriter(new FileWriter(System.getProperty("user.home")+"/data/genevariation/isoforms_length_analysis.log", true));
                    pw2.println(String.format("%s", uniProtId));
                    pw2.close();
                }

                for (GeneChromosomePosition gcp : gcps) {

                    if (! (gcp.getChromosome().equals(chr) && gcp.getGeneName().equals(geneSymbol)))
                        continue;

                    String geneBankId = gcp.getGenebankId();
                    if (gcp.getTranscriptionEnd() <= gcp.getCdsStart()) {
                        PrintWriter pw4 = new PrintWriter(new FileWriter(System.getProperty("user.home")+"/data/genevariation/isoforms_all_noncoding.log", true));
                        pw4.println(String.format("%s,%s,%s", gcp.getChromosome(), uniProtId, geneBankId));
                        pw4.close();
                        continue;
                    }

                    DNASequence transcriptDNASequence = ChromosomeMappingTools.getTranscriptDNASequence(twoBitFacade, gcp);
                    if (transcriptDNASequence.getSequenceAsString().equals("")) {
                        PrintWriter pw4 = new PrintWriter(new FileWriter(System.getProperty("user.home")+"/data/genevariation/isoforms_all_noncoding.log", true));
                        pw4.println(String.format("%s,%s,%s", gcp.getChromosome(), uniProtId, geneBankId));
                        pw4.close();
                        continue;
                    }
                    ProteinSequence sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptDNASequence);

                    int index = tools.getUniprotIsoformPositionByEqualSequence(isoforms, sequence);
                    PrintWriter pw3 = new PrintWriter(new FileWriter(System.getProperty("user.home")+"/data/genevariation/isoforms_all_coding.log", true));
                    pw3.println(String.format("%s,%s,%s,%d", gcp.getChromosome(), uniProtId, geneBankId, index));
                    pw3.close();

                    if (index == -1) {
                        PrintWriter pw1 = new PrintWriter(new FileWriter(System.getProperty("user.home")+"/data/genevariation/isoforms_mapping_analysis.log", true));
                        pw1.println(String.format("%s,%s", uniProtId, geneBankId));
                        pw1.close();
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }
}
