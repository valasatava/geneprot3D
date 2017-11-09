package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import scala.Tuple2;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class BuildAlternativeTranscripts implements PairFunction<Row, Row, GeneChromosomePosition>, Serializable {

    @Override
    public Tuple2<Row, GeneChromosomePosition> call(Row row) throws Exception
    {
        GeneChromosomePosition chromosomePosition = new GeneChromosomePosition();
        chromosomePosition
                .setChromosome(row.getString(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_CHROMOSOME)));
        chromosomePosition
                .setOrientation(row.getString(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_ORIENTATION))
                        .charAt(0));
        chromosomePosition
                .setTranscriptionStart(row.getInt(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_TX_START)));
        chromosomePosition
                .setTranscriptionEnd(row.getInt(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_TX_END)));
        chromosomePosition
                .setCdsStart(row.getInt(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_CDS_START)));
        chromosomePosition
                .setCdsEnd(row.getInt(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_CDS_END)));
        chromosomePosition
                .setExonStarts(row.getList(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_EXONS_START))
                        .stream().map(e->(Integer)e).collect(Collectors.toList()));
        chromosomePosition
                .setExonEnds(row.getList(row.schema().fieldIndex(org.rcsb.geneprot.common.utils.CommonConstants.COL_EXONS_END))
                        .stream().map(e->(Integer)e).collect(Collectors.toList()));
        return new Tuple2<>(row, chromosomePosition);
    }
}
