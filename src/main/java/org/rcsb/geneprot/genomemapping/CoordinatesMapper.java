package org.rcsb.geneprot.genomemapping;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class CoordinatesMapper {

    private static SparkSession sparkSession = SparkUtils.getSparkSession();

    /* Get mapping between NCBI RNA nucleotide accession, NCBI Reference Sequence protein accessions
     *  and UniProtKB protein accessions from UniProt database
     */
    public static Dataset<Row> getNCBIToUniProtKBAccessionDataset()
    {
        Dataset<Row> df = ExternalDBUtils.getNCBIAccessionstoIsofomMap();
        df = df
                .withColumn(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.ISOFORM_ID
                        , split(col(CommonConstants.MOLECULE_ID), CommonConstants.DASH).getItem(1));
        return df;
    }

    public static Dataset<Row> getGenomeAnnotation(String filePath)
    {
        sparkSession.sparkContext().addFile(filePath);
        int n = filePath.split("/").length;
        String filename = SparkFiles.get(filePath.split("/")[n - 1]);

        StructType schema = CommonConstants.GENOME_ANNOTATION_SCHEMA;
        schema.fieldIndex(CommonConstants.GENE_NAME);

        JavaRDD<Row> rdd =
                sparkSession.sparkContext().textFile(filename, 100)
                        .toJavaRDD()
                        .map(t -> t.split(CommonConstants.FIELD_SEPARATOR))
                        .map(t -> RowFactory.create(
                                t[schema.fieldIndex(CommonConstants.GENE_NAME)]
                                , t[schema.fieldIndex(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)]
                                , t[schema.fieldIndex(CommonConstants.CHROMOSOME)]
                                , t[schema.fieldIndex(CommonConstants.ORIENTATION)]
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.TX_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.TX_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.CDS_START)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.CDS_END)])
                                , Integer.valueOf(t[schema.fieldIndex(CommonConstants.EXONS_COUNT)])
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.EXONS_START)]
                                        .split(CommonConstants.EXONS_FIELD_SEPARATOR))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                , Arrays.stream(t[schema.fieldIndex(CommonConstants.EXONS_END)]
                                        .split(Pattern.quote(",")))
                                        .map(e -> Integer.valueOf(e)).collect(Collectors.toList()).toArray()
                                )
                        );

        Dataset<Row> df = sparkSession.createDataFrame(rdd, CommonConstants.GENOME_ANNOTATION_SCHEMA);
        return df;
    }

    public static Dataset<Row> annotateWithUniProtAccession(Dataset<Row> annotation)
    {
        Dataset<Row> accessions = getNCBIToUniProtKBAccessionDataset();
        annotation = annotation
                .join(accessions
                        , annotation.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION).equalTo(accessions.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                        , "left_outer")
                .drop(accessions.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION));
        return annotation;
    }

    public static Dataset<Row> getAlternativeProducts()
    {
        Dataset<Row> transcripts = getGenomeAnnotation(DataLocationProvider.getHumanGenomeAnnotationResource());
        transcripts = annotateWithUniProtAccession(transcripts);

        transcripts = transcripts
                .filter(col(CommonConstants.MOLECULE_ID).isNotNull()) // CHECK ISSUES WITH DB
                .groupBy(col(CommonConstants.CHROMOSOME), col(CommonConstants.GENE_NAME), col(CommonConstants.ORIENTATION), col(CommonConstants.UNIPROT_ID))
                .agg(collect_list(
                        struct(
                                  col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)
                                , col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION)
                                , col(CommonConstants.MOLECULE_ID)
                                , col(CommonConstants.ISOFORM_ID)
                                , col(CommonConstants.TX_START)
                                , col(CommonConstants.TX_END)
                                , col(CommonConstants.CDS_START)
                                , col(CommonConstants.CDS_END)
                                , col(CommonConstants.EXONS_COUNT)
                                , col(CommonConstants.EXONS_START)
                                , col(CommonConstants.EXONS_END)
                        )).as(CommonConstants.TRANSCRIPTS))
                .sort(col(CommonConstants.CHROMOSOME), col(CommonConstants.GENE_NAME));
        return transcripts;
    }

    public static List<Tuple2<Integer, Integer>> getCDSRegions(List<Integer> origExonStarts, List<Integer> origExonEnds, int cdsStart, int cdsEnd)
    {
        List<Integer> exonStarts = new ArrayList(origExonStarts);
        List<Integer> exonEnds = new ArrayList(origExonEnds);
        int j = 0;

        int nExons;
        for(nExons = 0; nExons < origExonStarts.size(); ++nExons) {
            if(((Integer)origExonEnds.get(nExons)).intValue() >= cdsStart && ((Integer)origExonStarts.get(nExons)).intValue() <= cdsEnd) {
                ++j;
            } else {
                exonStarts.remove(j);
                exonEnds.remove(j);
            }
        }

        nExons = exonStarts.size();
        exonStarts.remove(0);
        exonStarts.add(0, Integer.valueOf(cdsStart));
        exonEnds.remove(nExons - 1);
        exonEnds.add(Integer.valueOf(cdsEnd));

        List<Tuple2<Integer, Integer>> cdsRegion = new ArrayList();

        for(int i = 0; i < nExons; ++i) {
            Tuple2<Integer, Integer> r = new Tuple2(exonStarts.get(i), exonEnds.get(i));
            cdsRegion.add(r);
        }

        return cdsRegion;
    }

    public static void main(String[] args) {

        Dataset<Row> transcripts = getAlternativeProducts();
        //transcripts.show();

        JavaRDD<Object> rdd = transcripts
                .toJavaRDD()
                .map(new Function<Row, Object>() {

                    @Override
                    public Object call(Row row) throws Exception {

                        List<Row> annotations = row.getList(row.fieldIndex(CommonConstants.TRANSCRIPTS));
                        for (Row transcript : annotations) {

                            int cdsStart = transcript.getInt(transcript.fieldIndex(CommonConstants.CDS_START));
                            int cdsEnd = transcript.getInt(transcript.fieldIndex(CommonConstants.CDS_END));

                            List<Integer> exonsStart = transcript.getList(transcript.fieldIndex(CommonConstants.EXONS_START));
                            List<Integer> exonsEnd = transcript.getList(transcript.fieldIndex(CommonConstants.EXONS_END));

                            int mRNAPosStart = 1;
                            int mRNAPosEnd = 0;

                            List<Tuple2<Integer, Integer>> mRNAPositions = new ArrayList<>();
                            List<Tuple2<Integer, Integer>> isoformPositions = new ArrayList<>();
                            List<Tuple2<Integer, Integer>> cdsRegions = getCDSRegions(exonsStart, exonsEnd, cdsStart, cdsEnd);

                            for (Tuple2<Integer, Integer> cds : cdsRegions)
                            {
                                mRNAPosEnd = mRNAPosStart + cds._2-cds._1;

                                mRNAPositions.add(new Tuple2<>(mRNAPosStart, mRNAPosEnd));
                                isoformPositions.add(new Tuple2<>(mRNAPosStart/3 + 1, mRNAPosEnd/3));

                                mRNAPosStart = mRNAPosEnd+1;

                            }
                            System.out.println();
                        }

                        return null;
                    }
                });

        rdd.collect();

    }
}