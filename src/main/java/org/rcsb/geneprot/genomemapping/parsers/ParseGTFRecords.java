package org.rcsb.geneprot.genomemapping.parsers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.gencode.gtf.FeatureType;
import org.rcsb.geneprot.gencode.gtf.GencodeFeature;
import org.rcsb.geneprot.genomemapping.constants.DatasetSchemas;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yana Valasatava on 11/8/17.
 */
public class ParseGTFRecords implements Function<Iterable<GencodeFeature>, Row> {

    @Override
    public Row call(Iterable<GencodeFeature> features) throws Exception {

        StructType schema = DatasetSchemas.GENCODE_TRANSCRIPT_SCHEMA;
        Object[] t = new Object[schema.fields().length];

        for (GencodeFeature feature : features) {

            if (feature.getFeatureType().equals(FeatureType.TRANSCRIPT)) {

                t[schema.fieldIndex(CommonConstants.COL_CHROMOSOME)] = "chr"+feature.getChrom();
                t[schema.fieldIndex(CommonConstants.COL_GENE_NAME)] = feature.getGeneName();
                t[schema.fieldIndex(CommonConstants.COL_GENE_ID)] = feature.getAttributes().get("gene_id");
                t[schema.fieldIndex(CommonConstants.COL_ORIENTATION)] = feature.getStrand().toString();
                t[schema.fieldIndex(CommonConstants.COL_TRANSCRIPT_NAME)] = feature.getTranscriptName();
                t[schema.fieldIndex(CommonConstants.COL_TRANSCRIPT_ID)] = feature.getAttributes().get("transcript_id");

                t[schema.fieldIndex(CommonConstants.COL_CCDS_ID)] = feature.getAttributes().get("ccds_id");

                Object[] array = {feature.getStart(), feature.getEnd()};
                t[schema.fieldIndex(CommonConstants.COL_TRANSCRIPTION)] = RowFactory.create(array);

            } else if (feature.getFeatureType().equals(FeatureType.START_CODON)) {
                Object[] array = {feature.getStart(), feature.getEnd()};
                t[schema.fieldIndex(CommonConstants.COL_START_CODON)] = RowFactory.create(array);

            } else if (feature.getFeatureType().equals(FeatureType.STOP_CODON)) {
                Object[] array = {feature.getStart(), feature.getEnd()};
                t[schema.fieldIndex(CommonConstants.COL_STOP_CODON)] = RowFactory.create(array);
            }
        }

        List<Row> utr = new ArrayList<>();
        features.forEach(f -> { if ( f.getFeatureType().equals(FeatureType.UTR)
                                  || f.getFeatureType().equals(FeatureType.UTR5)
                                  || f.getFeatureType().equals(FeatureType.UTR3))
        {utr.add(RowFactory.create(new Object[]{f.getStart(), f.getEnd()}));}
        });
        t[schema.fieldIndex(CommonConstants.COL_UTR)] = utr.toArray();

        List<Row> cds = new ArrayList<>();
        features.forEach(f -> { if (f.getFeatureType().equals(FeatureType.CDS))
                {cds.add(RowFactory.create(new Object[]{Integer.valueOf(f.getAttributes().get("exon_number"))
                        , f.getStart(), f.getEnd()}));}
        });
        t[schema.fieldIndex(CommonConstants.COL_CODING)] = cds.toArray();

        List<Row> exons = new ArrayList<>();
        features.forEach(f -> {if(f.getFeatureType().equals(FeatureType.EXON)) {
            exons.add(RowFactory.create(new Object[]{Integer.valueOf(f.getAttributes().get("exon_number"))
                    , f.getStart(), f.getEnd()}));}
        });
        t[schema.fieldIndex(CommonConstants.COL_EXONS_COUNT)] = exons.size();
        t[schema.fieldIndex(CommonConstants.COL_EXONS)] = exons.toArray();

        return RowFactory.create(t);
    }
}