package org.rcsb.geneprot.transcriptomics.analysis;

import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.datastructures.ProteinFeatures;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.rcsb.geneprot.transcriptomics.properties.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Created by yana on 4/13/17.
 */
public class RunProteinSequenceCalc {

    private static String path = DataLocationProvider.getExonsProject();

    public static void getExonsAACharges(String chr, Encoder<ProteinFeatures> encoder, Dataset<Row> data) throws Exception {

            Dataset<ProteinFeatures> featuresDf = data.map(new MapToCharges(), encoder)
                    .filter(t->t!=null);
            List<String> features = featuresDf.map(new MapToChargesString(), Encoders.STRING()).collectAsList();

            String fpath = path + "DATA/amino_acid_charges_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(features, fpath);
    }

    public static void getExonsDisorderPrediction(String chr, Encoder<ProteinFeatures> encoder, Dataset<Row> data) throws Exception {

        Dataset<ProteinFeatures> featuresDf = data.map(new MapToProteinDisorder(), encoder)
                    .filter(t->t!=null);
        List<String> features = featuresDf.map(new MapToDisorderString(), Encoders.STRING()).collectAsList();

        String fpath = path + "DATA/disorder_prediction_" + chr + ".csv";
        org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(features, fpath);
    }

    public static void getExonsHydropathy(String chr, Encoder<ProteinFeatures> encoder, Dataset<Row> data) throws Exception {

            Dataset<ProteinFeatures> featuresDf = data.map(new MapToProteinHydropathy(), encoder)
                    .filter(t->t!=null);
            List<String> features = featuresDf.map(new MapToHydropathyString(), Encoders.STRING()).collectAsList();

            String fpath = path + "DATA/hydropathy_calculation_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(features, fpath);
    }

    public static void getExonsAAPolarity(String chr, Encoder<ProteinFeatures> encoder, Dataset<Row> data) throws Exception {

            Dataset<ProteinFeatures> featuresDf = data.map(new MapToPolarity(), encoder)
                    .filter(t->t!=null);
            List<String> features = featuresDf.map(new MapToPolarityString(), Encoders.STRING()).collectAsList();

            String fpath = path + "DATA/amino_acid_polarity_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(features, fpath);
    }

    public static void runAll(String exonsuniprotpath) throws Exception {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            System.out.println("Processing chromosome: "+chr);

            Encoder<ProteinFeatures> encoder = Encoders.bean(ProteinFeatures.class);
            Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsuniprotpath+"/"+chr);
            Dataset<ProteinFeatures> featuresDF = data.map(new MapToProteinFeatures(), encoder)
                    .filter(t->t!=null);
            featuresDF.persist();

            List<String> disorder = featuresDF.map(new MapToDisorderString(), Encoders.STRING()).collectAsList();
            String disorderfpath = path + "DATA/disorder_prediction_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(disorder, disorderfpath);

            List<String> hydropathy = featuresDF.map(new MapToHydropathyString(), Encoders.STRING()).collectAsList();
            String hydropathyfpath = path + "DATA/hydropathy_calculation_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(hydropathy, hydropathyfpath);

            List<String> charges = featuresDF.map(new MapToChargesString(), Encoders.STRING()).collectAsList();
            String chargesfpath = path + "DATA/amino_acid_charges_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(charges, chargesfpath);

            List<String> polarity = featuresDF.map(new MapToPolarityString(), Encoders.STRING()).collectAsList();
            String polarityfpath = path + "DATA/amino_acid_polarity_" + chr + ".csv";
            org.rcsb.geneprot.common.utils.CommonUtils.writeListOfStringsInFile(polarity, polarityfpath);
        }
    }
}
