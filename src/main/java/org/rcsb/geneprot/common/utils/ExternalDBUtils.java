package org.rcsb.geneprot.common.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.redwood.util.DBUtils;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class ExternalDBUtils {

    public static Dataset<Row> getGeneNamesForOrganism(int taxonomyId)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("gnt.value_ as " + org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME +", ");
        sb.append("gnt.TYPE_ as " + org.rcsb.mojave.util.CommonConstants.COL_TYPE +" ");
        sb.append("from entry_accession as ea, gene_type as g, gene_name_type as gnt, " +
                "organism_name_type as ont, entry as e, organism_type as ot, db_reference_type as dbref ");
        sb.append("where (ea.HJID=g.GENE_ENTRY_HJID) ");
        sb.append("and g.HJID = gnt.NAME__GENE_TYPE_HJID ");
        sb.append("and (ea.HJID=e.HJID) ");
        sb.append("and (e.ORGANISM_ENTRY_HJID=ot.HJID) ");
        sb.append("and (ot.HJID=ont.NAME__ORGANISM_TYPE_HJID) ");
        sb.append("and (dbref.DB_REFERENCE_ORGANISM_TYPE_H_0=ot.HJID) ");
        sb.append("and dbref.ID="+String.valueOf(taxonomyId));

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getNCBIAccessionstoIsofomMap()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("pt.VALUE_ as " + CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION + ", ");
        sb.append("dbr.ID as " + CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION + ", ");
        sb.append("mt.ID as "+CommonConstants.MOLECULE_ID+" ");
        sb.append("from entry_accession as ea " +
                "LEFT JOIN db_reference_type as dbr on ( ea.HJID=dbr.DB_REFERENCE_ENTRY_HJID ) " +
                "LEFT JOIN property_type as pt on ( dbr.HJID=pt.PROPERTY_DB_REFERENCE_TYPE_H_0 ) " +
                "LEFT JOIN molecule_type as mt on ( mt.HJID=dbr.MOLECULE_DB_REFERENCE_TYPE_H_0 ) ");
        sb.append("where ( dbr.TYPE_='RefSeq') ");
        sb.append("and ( pt.TYPE_='nucleotide sequence ID') ");
        sb.append("and ( ea.HJINDEX=0) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static void main(String[] args)
    {
        Dataset<Row> df = getNCBIAccessionstoIsofomMap();
        df.filter(col("UniProtId").equalTo("P01023")).show();

        df = df
                .withColumn(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION
                        , split(col(CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION), CommonConstants.DOT).getItem(0))
                .withColumn(CommonConstants.ISOFORM_ID
                        , split(col(CommonConstants.MOLECULE_ID), CommonConstants.DASH).getItem(1))
                ;
        df.filter(col("UniProtId").equalTo("P01023")).show();
    }
}
