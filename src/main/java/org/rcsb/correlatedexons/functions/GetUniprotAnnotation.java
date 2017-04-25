package org.rcsb.correlatedexons.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.correlatedexons.mappers.MapToUniprotFeatures;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.rcsb.uniprot.auto.Entry;
import org.rcsb.uniprot.auto.FeatureType;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.config.RCSBUniProtMirror;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;

/**
 * Created by yana on 4/25/17.
 */
public class GetUniprotAnnotation {

    public static void foo() throws FileNotFoundException, JAXBException {

        String uniProtId = "Q8V5E0";
        Uniprot up = RCSBUniProtMirror.getUniProtFromFile(uniProtId);

        for(Entry e : up.getEntry()){
            for (FeatureType ft : e.getFeature()){

                // ranged features
                if ( ft.getLocation() != null && ft.getLocation().getBegin() != null && ft.getLocation().getEnd() != null)
                    System.out.println("RANGED: " + ft.getDescription()+" --- "+
                            ft.getType() + " --- " +
                            ft.getLocation().getBegin().getPosition() + " : " + ft.getLocation().getEnd().getPosition());

                // single resiude position features
                if ( ft.getLocation() != null && ft.getLocation().getPosition() != null && ft.getLocation().getPosition().getPosition() != null)
                    System.out.println("FT: " + ft.getDescription()+" --- "+
                            ft.getType() + " --- " +
                            ft.getLocation().getPosition().getPosition());
            }
        }
    }

    public static void run(String chr) throws FileNotFoundException, JAXBException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getExonsUniprotLocation() + "/" + chr);

        JavaRDD<Row> data = mapping
                .drop(mapping.col("isoformIndex")).drop(mapping.col("isoformPosStart")).drop(mapping.col("isoformPosEnd"))
                .toJavaRDD().map(new MapToUniprotFeatures());
        data.collect();

    }

    public static void main(String[] args) throws FileNotFoundException, JAXBException {

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
                "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",
                "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            System.out.println("Prosessing "+chr);

            run(chr);

            System.out.println("done");
        }
    }
}
