package org.rcsb.geneprot.genomemapping.parsers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.gencode.gtf.FeatureType;
import org.rcsb.geneprot.gencode.gtf.GencodeFeature;

import java.util.ArrayList;

/**
 * Created by Yana Valasatava on 11/8/17.
 */
public class ParseGTFRecords implements Function<Iterable<GencodeFeature>, Row> {

    @Override
    public Row call(Iterable<GencodeFeature> features) throws Exception {

        StructType schema = CommonConstants.GENOME_ANNOTATION_SCHEMA;
        Object[] t = new Object[schema.fields().length];

        for (GencodeFeature feature : features) {

            if (feature.getFeatureType().equals(FeatureType.TRANSCRIPT)) {
                t[schema.fieldIndex(CommonConstants.COL_CHROMOSOME)] = "chr"+feature.getChrom();
                t[schema.fieldIndex(CommonConstants.COL_GENE_NAME)] = feature.getGeneName();
                t[schema.fieldIndex(CommonConstants.COL_ORIENTATION)] = feature.getStrand().toString();
                t[schema.fieldIndex(CommonConstants.COL_TX_START)] = feature.getStart();
                t[schema.fieldIndex(CommonConstants.COL_TX_END)] = feature.getEnd();
            }
        }

        ArrayList<Integer> startPosExons = new ArrayList<>();
        features.forEach(f -> {
            if(f.getFeatureType().equals(FeatureType.EXON)) {
                startPosExons.add(Integer.valueOf(f.getAttributes().get("start")));
            }
        });
        t[schema.fieldIndex(CommonConstants.COL_EXONS_START)] = startPosExons.toArray();

        ArrayList<Integer> endPosExons = new ArrayList<>();
        features.forEach(f -> {
            if(f.getFeatureType().equals(FeatureType.EXON)) {
                endPosExons.add(Integer.valueOf(f.getAttributes().get("end")));
            }
        });
        t[schema.fieldIndex(CommonConstants.COL_EXONS_END)] = endPosExons.toArray();

//        return RowFactory.create(
//                t[schema.fieldIndex(CommonConstants.COL_GENE_NAME)]
//                , t[schema.fieldIndex(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION)]
//                , t[schema.fieldIndex(CommonConstants.COL_CHROMOSOME)]
//                , t[schema.fieldIndex(CommonConstants.COL_ORIENTATION)]
//                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_START)])
//                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_TX_END)])
//                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_START)])
//                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_CDS_END)])
//                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.COL_EXONS_COUNT)])
//                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_START)]
//                        .split(CommonConstants.EXONS_FIELD_SEPARATOR))
//                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
//                , Arrays.stream(t[schema.fieldIndex(CommonConstants.COL_EXONS_END)]
//                        .split(Pattern.quote(",")))
//                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
//        );

        return null;
    }
}
