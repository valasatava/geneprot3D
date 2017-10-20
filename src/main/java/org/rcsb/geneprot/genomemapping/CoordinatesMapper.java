package org.rcsb.geneprot.genomemapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.functions.MapGenomeToUniProt;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;

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
    public static Dataset<Row> getNCBIToMoleculeIdAccessionDataset()
    {
        Dataset<Row> df = ExternalDBUtils.getNCBIAccessionsToIsofomsMap();
        df = df
                .withColumn(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.ISOFORM_ID
                        , split(col(CommonConstants.MOLECULE_ID), CommonConstants.DASH).getItem(1));
        return df;
    }

    public static Dataset<Row> getNCBIToUniProtAccessionDataset()
    {
        Dataset<Row> df = ExternalDBUtils.getNCBItoUniProtAccessionsMap();
        df = df
                .withColumn(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0));
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
        //Dataset<Row> accessions = getNCBIToUniProtKBAccessionDataset();
        Dataset<Row> accessions = getNCBIToUniProtAccessionDataset();
        annotation.show();
        accessions.show();
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
        transcripts = annotateWithUniProtAccession(transcripts).cache();

        long assigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNotNull()).count();
        System.out.println("assigned: "+assigned);

        long notassigned = transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNull()).count();
        System.out.println("not assigned: "+notassigned);

        transcripts
                .filter(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION).isNull()).show(100);

        transcripts = transcripts
                .filter(col(CommonConstants.MOLECULE_ID).isNotNull()) // CHECK ISSUES WITH DB
                .groupBy(col(CommonConstants.CHROMOSOME), col(CommonConstants.GENE_NAME), col(CommonConstants.ORIENTATION), col(CommonConstants.UNIPROT_ID))
                .agg(collect_list(
                        struct(   col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)
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

    public static void writeListToMongo(List<GenomeToUniProtMapping> list) throws Exception
    {
        int bulkSize = 10000;
        int count = 0;

        MongoClient mongoClient = new MongoClient("132.249.213.154");
        DB db = mongoClient.getDB("dw_v1");
        DBCollection collection = db.getCollection("humanGenomeMapping");

        ObjectMapper mapper = new ObjectMapper();

        BulkWriteOperation bulkOperation;
        try {
            bulkOperation = collection.initializeUnorderedBulkOperation();

            for (GenomeToUniProtMapping object : list) {

                DBObject dbo = mapper.convertValue(object, BasicDBObject.class);

                bulkOperation.insert(dbo);
                count++;

                if (count >= bulkSize) {
                    //time to perform the bulk insert
                    bulkOperation.execute();
                    count = 0;
                    bulkOperation = collection.initializeUnorderedBulkOperation();
                }

            }
            //finish up the last few
            if (count > 0) {
                bulkOperation.execute();
            }

        } catch (RuntimeException e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {

        Dataset<Row> transcripts = getAlternativeProducts();
        //transcripts = transcripts.filter(col(CommonConstants.GENE_NAME).equalTo("MAGI3"));

        JavaRDD<GenomeToUniProtMapping> rdd = transcripts
                .toJavaRDD()
                .map(new MapGenomeToUniProt());

        List<GenomeToUniProtMapping> list = rdd.collect();
        writeListToMongo(list);

//        Dataset<Row> dataset = sparkSession.createDataset(rdd, CommonConstants.GENOME_MAPPING_SCHEMA);
//        dataset.persist(StorageLevel.MEMORY_AND_DISK());
//        dataset.printSchema();
//        DerivedDataLoadUtils.writeToMongo(dataset, "humanGenomeMapping", SaveMode.Overwrite);
    }
}