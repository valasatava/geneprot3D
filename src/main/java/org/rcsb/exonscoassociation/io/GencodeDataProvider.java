package org.rcsb.exonscoassociation.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.exonscoassociation.utils.ExonsUtils;
import org.rcsb.genevariation.constants.ExonFrameOffset;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Exon;
import org.rcsb.genevariation.datastructures.Gene;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 5/11/17.
 */
public class GencodeDataProvider {

    private static String DEFAULT_MAPPING = DataLocationProvider.getExonsProject()+"/GENCODE_DATA/gencode.v24.CDS.protein_coding_exons.gtf";
    private Dataset<Row> annotation;

    public void getAnnotation() {
        annotation = SaprkUtils.getSparkSession().read().csv(DEFAULT_MAPPING);
        annotation.persist(StorageLevel.MEMORY_AND_DISK());
    }

    public void annotateGene( Gene gene ) {

        Map<String, Iterable<Row>> geneData = annotation.filter(col("_c6").contains(gene.getEnsembleId()))
                .toJavaRDD().groupBy(t -> t.getString(5)).collectAsMap();

        boolean forward = true;
        if (gene.getOrientation().equals(StrandOrientation.REVERSE)) {
            forward = false;
        }

        List<Transcript> transcripts = new ArrayList<>();
        for ( Map.Entry<String, Iterable<Row>> tr : geneData.entrySet() ) {

            Transcript transcript = new Transcript(gene);
            transcript.setGeneBankId(tr.getKey());

            List<Exon> exons = new ArrayList<>();
            Iterator<Row> exonsIt = tr.getValue().iterator();

            while (exonsIt.hasNext()) {

                Row e = exonsIt.next();

                Exon exon = new Exon();
                exon.setStart(Integer.valueOf(e.getString(1)));
                exon.setEnd(Integer.valueOf(e.getString(2)));
                exon.setPhase(Integer.valueOf(e.getString(4)));
                exon.setFrame(Integer.valueOf(e.getString(4)), true);
                exons.add(exon);
            }

            ExonsUtils.orderExons(exons);
            transcript.setExons(exons);
            transcripts.add(transcript);
        }
        gene.setTranscripts(transcripts);
    }

    public int[] getExonOffsets(String chr, int start, int end) {

        List<Row> exons = annotation.filter(col("_c0").equalTo(chr)
                .and(col("_c1").equalTo(start).and(col("_c2").equalTo(end))))
                .drop("_c3").drop("_c5").drop("_c6").distinct().collectAsList();

        int[] offsets = new int[exons.size()];
        for ( int i=0; i<exons.size(); i++ ) {
            offsets[i]= Integer.valueOf(exons.get(i).getString(3));
        }
        return offsets;
    }
}
