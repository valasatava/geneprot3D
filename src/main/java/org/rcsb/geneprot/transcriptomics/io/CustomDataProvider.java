package org.rcsb.geneprot.transcriptomics.io;

import com.google.common.collect.Range;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.genome.parsers.twobit.SimpleTwoBitFileProvider;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.rcsb.geneprot.genes.constants.StrandOrientation;
import org.rcsb.geneprot.genes.datastructures.Exon;
import org.rcsb.geneprot.genes.datastructures.Gene;
import org.rcsb.geneprot.genes.datastructures.Transcript;
import org.rcsb.geneprot.genes.expression.RNApolymerase;
import org.rcsb.geneprot.genes.expression.Ribosome;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.transcriptomics.datastructures.ExonBoundariesPair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 5/11/17.
 */
public class CustomDataProvider {

    public static Range<Integer> getBoundaries(String e) {
        return Range.closed(Integer.valueOf(e.split("_")[1]),Integer.valueOf(e.split("_")[2]));
    }

    public static void runBothIn(String filepath) throws Exception {

        FileWriter writer = new FileWriter(DataLocationProvider.getExonsProjectResults()+"translated.fasta");

        List<String> done = Files.readAllLines(Paths.get(DataLocationProvider.getExonsProjectResults()+"done"));

        File twoBitFileLocalLocation = new File(DataLocationProvider.getGenomeLocation());
        SimpleTwoBitFileProvider.downloadIfNoTwoBitFileExists(twoBitFileLocalLocation, "hg38");
        TwoBitFacade twoBitFacade = new TwoBitFacade(twoBitFileLocalLocation);
        
        GencodeDataProvider genecode = new GencodeDataProvider();
        genecode.getAnnotation();

        List<String> lines = Files.readAllLines(Paths.get(filepath));

        for (String line : lines ) {

            String[] fields = line.split("\t");

            Gene gene = new Gene();
            gene.setChromosome(fields[0].split("_")[0]);
            gene.setEnsembleId(fields[2]);

            gene.setOrientation(StrandOrientation.FORWARD);
            if (fields[0].split("_")[3].equals("-")) {
                gene.setOrientation(StrandOrientation.REVERSE);
            }
            genecode.annotateGene(gene);

            Range<Integer> ase1 = getBoundaries(fields[0]);
            Range<Integer> ase2 = getBoundaries(fields[1]);

            for ( Transcript t : gene.getTranscripts() ) {

                if ( done.contains(t.getGeneBankId().replace(";",""))) {
                    continue;
                }

                int ind1 = t.getExonIndexByStartPos(ase1.lowerEndpoint());
                int ind2 = t.getExonIndexByStartPos(ase2.lowerEndpoint());

                int i1=0; int i2=0;
                int id1=0; int id2=0;
                if ( (ind1==-1 || ind2==-1) ) {
                    continue;
                }
                else {
                    i1 = ind1 - 1;
                    id1 = 1;
                    i2 = ind2 + 2;
                    id2= ind2-ind1;
                }
                if ( ind1==0 ) {
                    i1 = ind1;
                    id1 = 0;
                }
                if ( ind2 == t.getNumberOfExons()-1 ) {
                    i2 = ind2+1;
                    id2 = ind2-ind1+1;
                }

                // Both are included
                List<Exon> exons = t.getExons().subList(i1, i2);
                DNASequence dnaSequenceBothIn = RNApolymerase.getCodingSequence(twoBitFacade, gene.getChromosome(),
                            gene.getOrientation(), exons);

                ProteinSequence sequenceBothIn;
                if (gene.getOrientation().equals(StrandOrientation.FORWARD)) {
                    sequenceBothIn = Ribosome.getProteinSequence(dnaSequenceBothIn.getRNASequence(exons.get(0).getFrame()));
                }
                else {
                    sequenceBothIn = Ribosome.getProteinSequence(dnaSequenceBothIn.getRNASequence(exons.get(exons.size()-1).getFrame()));
                }
                sequenceBothIn.setDescription(">"+t.getGeneBankId()+fields[0]+";"+fields[1]+";bothIn");

                System.out.println(sequenceBothIn.getDescription());
                writer.write(sequenceBothIn.getDescription()+"\n");
                writer.write(sequenceBothIn.getSequenceAsString()+"\n");

                // Both are excluded
                exons.remove(1);
                exons.remove(exons.size()-2);
                DNASequence dnaSequenceBothOut = RNApolymerase.getCodingSequence(twoBitFacade, gene.getChromosome(),
                            gene.getOrientation(), exons);

                ProteinSequence sequenceBothOut;
                if (gene.getOrientation().equals(StrandOrientation.FORWARD)) {
                    sequenceBothOut = Ribosome.getProteinSequence(dnaSequenceBothOut.getRNASequence(exons.get(0).getFrame()));
                }
                else {
                    sequenceBothOut = Ribosome.getProteinSequence(dnaSequenceBothOut.getRNASequence(exons.get(exons.size()-1).getFrame()));
                }
                sequenceBothOut.setDescription(">"+t.getGeneBankId()+fields[0]+";"+fields[1]+";bothOut");

                System.out.println(sequenceBothOut.getDescription());
                writer.write(sequenceBothOut.getDescription()+"\n");
                writer.write(sequenceBothOut.getSequenceAsString()+"\n");

                writer.flush();
            }
        }
        writer.close();
    }

    public static void main(String args[]) throws Exception {
        runBothIn(DataLocationProvider.getExonsProject()+"DATA/bothIn.tab");
    }

    public static List<ExonBoundariesPair> getCoordinatedPairs(Path path) throws IOException {

        List<ExonBoundariesPair> pairs = new ArrayList<>();
        List<String> lines = Files.readAllLines(path);
        for ( String line : lines ) {
            String chr = line.split(",")[0].split("_")[0];
            String e1=line.split(",")[0].split("_")[1]+"_"+line.split(",")[0].split("_")[2];
            String e2=line.split(",")[1].split("_")[1]+"_"+line.split(",")[1].split("_")[2];
            ExonBoundariesPair ebp = new ExonBoundariesPair(chr, e1, e2);
            pairs.add(ebp);
        }
        return pairs;
    }
}
