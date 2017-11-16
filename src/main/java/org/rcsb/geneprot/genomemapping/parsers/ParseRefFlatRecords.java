package org.rcsb.geneprot.genomemapping.parsers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.DatasetSchemas;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Yana Valasatava on 11/8/17.
 */
public class ParseRefFlatRecords implements Function<String, Row> {

    @Override
    public Row call(String s) throws Exception {

        StructType schema = DatasetSchemas.GENOME_ANNOTATION_SCHEMA;
        String[] t = s.split(CommonConstants.FIELD_SEPARATOR);
        return RowFactory.create(
                t[schema.fieldIndex(CommonConstants.COL_GENE_NAME)]
                , t[schema.fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)]
                , t[schema.fieldIndex(CommonConstants.COL_CHROMOSOME)]
                , t[schema.fieldIndex(CommonConstants.COL_ORIENTATION)]
                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_START)])
                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_END)])
                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_START)])
                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_END)])
                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_EXONS_COUNT)])
                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_START)]
                        .split(CommonConstants.EXONS_FIELD_SEPARATOR))
                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_END)]
                        .split(Pattern.quote(",")))
                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                );
    }
}
