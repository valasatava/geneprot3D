package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class MapTranscriptToIsoform implements PairFunction<Tuple2<Row, String>, Row, String>{

    @Override
    public Tuple2<Row, String> call(Tuple2<Row, String> t) throws Exception {

        Row e = t._1;
        String up = e.getString(e.schema().fieldIndex(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION));

        return null;
    }
}
