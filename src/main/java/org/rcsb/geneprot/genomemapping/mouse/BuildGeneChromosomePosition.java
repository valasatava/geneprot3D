package org.rcsb.geneprot.genomemapping.mouse;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.rcsb.humangenome.function.SparkGeneChromosomePosition;
import scala.Tuple2;

/**
 */
public class BuildGeneChromosomePosition implements PairFunction<GeneChromosomePosition, String, SparkGeneChromosomePosition> {

    @Override
    public Tuple2<String, SparkGeneChromosomePosition> call(GeneChromosomePosition chromosomePosition) throws Exception
    {
        String chromosome = chromosomePosition.getChromosome();
        SparkGeneChromosomePosition n = SparkGeneChromosomePosition.fromGeneChromosomePosition(chromosomePosition);
        Tuple2<String, SparkGeneChromosomePosition> t2 = new Tuple2(chromosome, n);
        return t2;
    }
}
