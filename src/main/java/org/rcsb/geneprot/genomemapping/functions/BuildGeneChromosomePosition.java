package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.rcsb.humangenome.function.SparkGeneChromosomePosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 */
public class BuildGeneChromosomePosition implements PairFlatMapFunction<String, String, SparkGeneChromosomePosition> {

    private static final Logger PdbLogger = LoggerFactory.getLogger(BuildGeneChromosomePosition.class);

    private static File f;
    public BuildGeneChromosomePosition(File geneToPositionFile)
    {
        f = geneToPositionFile;
    }

    private String getExonStrings(GeneChromosomePosition g) {
        return g.getExonStarts().toString() + g.getExonEnds().toString();
    }

    private List<GeneChromosomePosition> mapGeneNames2Position(File f) throws IOException {

        PdbLogger.info("Parsing " + f.getAbsolutePath());

        Map<String, List<GeneChromosomePosition>> geneChromosomePositionMap = new HashMap();

        InputStream instream2 = new GZIPInputStream(new FileInputStream(f));
        GeneChromosomePositionParser parser = new GeneChromosomePositionParser();
        List<GeneChromosomePosition> genePositions = parser.getChromosomeMappings(instream2);
        Iterator var7 = genePositions.iterator();

        while(var7.hasNext()) {
            GeneChromosomePosition pos = (GeneChromosomePosition)var7.next();
            List<GeneChromosomePosition> posMap = (List)geneChromosomePositionMap.get(pos.getGeneName());
            if(posMap == null) {
                posMap = new ArrayList();
                geneChromosomePositionMap.put(pos.getGeneName(), posMap);
            }

            String exonS1 = this.getExonStrings(pos);
            boolean found = false;
            Iterator var12 = ((List)posMap).iterator();

            while(var12.hasNext()) {
                GeneChromosomePosition g = (GeneChromosomePosition)var12.next();
                String exonS2 = this.getExonStrings(g);
                if(exonS1.equals(exonS2)) {
                    found = true;
                    break;
                }
            }

            if(!found) {
                ((List)posMap).add(pos);
            }
        }

        PdbLogger.info("Got " + geneChromosomePositionMap.keySet().size() + " gene positions");
        return genePositions;
    }

    @Override
    public Iterator<Tuple2<String, SparkGeneChromosomePosition>> call(String geneSymbol) throws Exception {

        List<Tuple2<String, SparkGeneChromosomePosition>> data = new ArrayList<>();

        List<GeneChromosomePosition> chromosomePositions = mapGeneNames2Position(f);

        if (chromosomePositions == null || chromosomePositions.size() == 0) {
            return data.iterator();
        }

        for ( GeneChromosomePosition chromosomePosition : chromosomePositions) {

            String chromosome = chromosomePosition.getChromosome();

            SparkGeneChromosomePosition n = SparkGeneChromosomePosition.fromGeneChromosomePosition(chromosomePosition);

            Tuple2<String, SparkGeneChromosomePosition> t2 = new Tuple2(chromosome,n);
            data.add(t2);
        }
        return data.iterator();
    }
}
