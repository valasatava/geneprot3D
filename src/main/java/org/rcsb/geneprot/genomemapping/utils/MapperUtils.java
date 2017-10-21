package org.rcsb.geneprot.genomemapping.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.split;

/**
 * Created by Yana Valasatava on 10/20/17.
 */
public class MapperUtils {

    public static void main(String[] args) {
    }

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

    public static Dataset<Row> getGeneNameToUniProtAccessionDataset() {

        Dataset<Row> df = ExternalDBUtils.getGeneNameToUniProtAccessionsMap();
        df = df
                .groupBy(col(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION))
                .agg(collect_set(col(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME)));

        return df;
    }

    public static Dataset<Row> mapTranscriptsToUniProtAccession(Dataset<Row> annotation)
    {
        Dataset<Row> accessions = getNCBIToMoleculeIdAccessionDataset();

        annotation = annotation
                .join(accessions
                        , annotation.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION)
                                .equalTo(accessions.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION))
                        , "left_outer")
                .drop(accessions.col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION));
        return annotation;
    }

    public static Dataset<Row> mapGenesToUniProtAccession(Dataset<Row> annotation)
    {
        Dataset<Row> accessions = ExternalDBUtils.getGeneNameToUniProtAccessionsMap();
        annotation = annotation
                .join(accessions
                        , annotation.col(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME)
                                .equalTo(accessions.col(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME))
                        , "left_outer")
                .drop(accessions.col(org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME));
        return annotation;
    }
}
