package org.rcsb.geneprot.common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.genomemapping.model.GenomeToUniProtMapping;
import org.rcsb.redwood.util.DBUtils;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class ExternalDBUtils {

    public static Dataset<Row> getAccessionsForOrganism(int taxonomyId)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + " ");
        sb.append("from entry_accession as ea, ");
        sb.append("entry as e, ");
        sb.append("organism_type as ot, ");
        sb.append("db_reference_type as dbref ");
        sb.append("where (ea.HJID=e.HJID) ");
        sb.append("and (e.ORGANISM_ENTRY_HJID=ot.HJID) ");
        sb.append("and (dbref.DB_REFERENCE_ORGANISM_TYPE_H_0=ot.HJID) ");
        sb.append("and dbref.ID="+String.valueOf(taxonomyId)+" ");
        sb.append("and ( ea.HJINDEX=0) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

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

    public static Dataset<Row> getGeneNameToUniProtAccessionsMap()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("gnt.VALUE_ as " + org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME + " ");
        sb.append("from entry_accession as ea " +
                "JOIN gene_type as gt on ( ea.HJID=gt.GENE_ENTRY_HJID ) " +
                "JOIN gene_name_type as gnt on ( gt.HJID=gnt.NAME__GENE_TYPE_HJID ) ");
        sb.append("where ( gnt.TYPE_='primary') ");
        sb.append("and ( ea.HJINDEX=0) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getGeneNameToUniProtAccessionsMap(int taxonomyId)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("gnt.VALUE_ as " + org.rcsb.mojave.util.CommonConstants.COL_GENE_NAME + " ");
        sb.append("from entry_accession as ea ");
        sb.append("JOIN entry as e on (ea.HJID=e.HJID)");
        sb.append("JOIN gene_type as gt on ( ea.HJID=gt.GENE_ENTRY_HJID ) ");
        sb.append("JOIN gene_name_type as gnt on ( gt.HJID=gnt.NAME__GENE_TYPE_HJID ) ");
        sb.append("JOIN organism_type as ot on (e.ORGANISM_ENTRY_HJID=ot.HJID) ");
        sb.append("JOIN db_reference_type as dbref on (dbref.DB_REFERENCE_ORGANISM_TYPE_H_0=ot.HJID) ");
        sb.append("where ( gnt.TYPE_='primary') ");
        sb.append("and ( ea.HJINDEX=0) ");
        sb.append("and dbref.ID="+String.valueOf(taxonomyId));

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getNCBItoUniProtAccessionsMap()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("pt.VALUE_ as " + CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION + ", ");
        sb.append("dbr.ID as " + CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION + " ");
        sb.append("from entry_accession as ea " +
                "LEFT JOIN db_reference_type as dbr on ( ea.HJID=dbr.DB_REFERENCE_ENTRY_HJID ) " +
                "LEFT JOIN property_type as pt on ( dbr.HJID=pt.PROPERTY_DB_REFERENCE_TYPE_H_0 ) ");
        sb.append("where ( dbr.TYPE_='RefSeq') ");
        sb.append("and ( pt.TYPE_='nucleotide sequence ID') ");
        sb.append("and ( ea.HJINDEX=0) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getNCBIAccessionsToIsofomsMap()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("pt.VALUE_ as " + CommonConstants.NCBI_RNA_SEQUENCE_ACCESSION + ", ");
        sb.append("dbr.ID as " + CommonConstants.NCBI_PROTEIN_SEQUENCE_ACCESSION + ", ");
        sb.append("mt.ID as "+CommonConstants.COL_MOLECULE_ID +" ");
        sb.append("from entry_accession as ea " +
                "LEFT JOIN db_reference_type as dbr on ( ea.HJID=dbr.DB_REFERENCE_ENTRY_HJID ) " +
                "LEFT JOIN property_type as pt on ( dbr.HJID=pt.PROPERTY_DB_REFERENCE_TYPE_H_0 ) " +
                "LEFT JOIN molecule_type as mt on ( mt.HJID=dbr.MOLECULE_DB_REFERENCE_TYPE_H_0 ) ");
        sb.append("where ( dbr.TYPE_='RefSeq') ");
        sb.append("and ( pt.TYPE_='nucleotide sequence ID') ");
        sb.append("and ( ea.HJINDEX=0) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getCanonicalUniProtSequence(String uniProtId)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select st.VALUE_ as " + CommonConstants.COL_PROTEIN_SEQUENCE + " ");
        sb.append("from entry_accession as ea JOIN sequence_type as st on ( ea.HJID=st.HJID ) ");
        sb.append("where ( ea.hjvalue='"+uniProtId+"') ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getSequenceComments(String uniProtId)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select its.REF_ as "+ CommonConstants.COL_FEATURE_ID + ", ");
        sb.append("its.TYPE_ as " + CommonConstants.COL_SEQUENCE_TYPE + ", ");
        sb.append("iti.HJVALUE as " + CommonConstants.COL_MOLECULE_ID + " ");
        sb.append("from isoform_type_sequence AS its JOIN isoform_type AS it ON (its.HJID=it.SEQUENCE__ISOFORM_TYPE_HJID) ");
        sb.append("JOIN isoform_type_id AS iti ON (iti.HJID=it.HJID) ");
        sb.append("JOIN comment_type AS ct ON (ct.HJID=it.ISOFORM_COMMENT_TYPE_HJID) ");
        sb.append("JOIN entry AS e ON (e.HJID=ct.COMMENT__ENTRY_HJID) ");
        sb.append("JOIN entry_accession AS ea ON (ea.HJID=e.HJID) ");
        sb.append("where ( ea.HJVALUE='"+uniProtId+"') ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }

    public static Dataset<Row> getCanonicalSequenceFeatures()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select distinct ea.hjvalue as "+ CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("st.VALUE_ as " + CommonConstants.COL_PROTEIN_SEQUENCE + " ");
        sb.append("from entry_accession AS ea ");
        sb.append("JOIN entry as e on (ea.HJID=e.HJID)");
        sb.append("JOIN sequence_type as st on ( e.SEQUENCE__ENTRY_HJID=st.HJID ) ");
        sb.append("LEFT OUTER JOIN comment_type AS ct ON (ea.HJID=ct.COMMENT__ENTRY_HJID) ");
        sb.append("LEFT OUTER JOIN isoform_type AS it ON (ct.HJID=it.ISOFORM_COMMENT_TYPE_HJID) ");
        sb.append("where ( (ea.HJINDEX=0 ");
        sb.append("and ISOFORM_COMMENT_TYPE_HJID is NULL) ");
        sb.append("or COMMENT__ENTRY_HJID is NULL) ");

        return DBUtils.executeSQLsourceUniprot("(" + sb.toString() + ") as tbl");
    }

    public static Dataset<Row> getIsoformSequenceFeatures()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ea.hjvalue as "+ CommonConstants.COL_UNIPROT_ACCESSION + ", ");
        sb.append("st.VALUE_ as " + CommonConstants.COL_PROTEIN_SEQUENCE + ", ");
        sb.append("its.TYPE_ as " + CommonConstants.COL_SEQUENCE_TYPE + ", ");
        sb.append("its.REF_ as "+ CommonConstants.COL_FEATURE_ID + ", ");
        sb.append("iti.HJVALUE as " + CommonConstants.COL_MOLECULE_ID + " ");
        sb.append("from entry_accession AS ea ");
        sb.append("JOIN sequence_type as st on ( ea.HJID=st.HJID ) ");
        sb.append("JOIN comment_type AS ct ON (ea.HJID=ct.COMMENT__ENTRY_HJID) ");
        sb.append("JOIN isoform_type AS it ON (ct.HJID=it.ISOFORM_COMMENT_TYPE_HJID) ");
        sb.append("JOIN isoform_type_sequence AS its ON (its.HJID=it.SEQUENCE__ISOFORM_TYPE_HJID) ");
        sb.append("JOIN isoform_type_id AS iti ON (iti.HJID=it.HJID) ");
        sb.append("where ( ea.HJINDEX=0 ) ");
        sb.append("and ( ct.TYPE_ ='alternative products' ) ");

        return DBUtils.executeSQLsourceUniprot("("+sb.toString()+") as tbl");
    }
    
    public static Dataset<Row> getSequenceVariationsInRanges()
    {
        StringBuffer sb1 = new StringBuffer();
        sb1.append("select ft.ID as "+ CommonConstants.COL_FEATURE_ID + ", ");
        sb1.append("ft.ORIGINAL as "+ CommonConstants.COL_ORIGINAL + ", ");
        sb1.append("ftv.HJVALUE as "+ CommonConstants.COL_VARIATION + ", ");
        sb1.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_BEGIN + " ");
        sb1.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.BEGIN__LOCATION_TYPE_HJID) ");
        sb1.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb1.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb1.append("where ( ft.TYPE_='splice variant') ");
        Dataset<Row> df1 = DBUtils.executeSQLsourceUniprot("(" + sb1.toString() + ") as tbl");

        StringBuffer sb2 = new StringBuffer();
        sb2.append("select ft.ID as "+ CommonConstants.COL_FEATURE_ID + ", ");
        sb2.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_END + " ");
        sb2.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.END__LOCATION_TYPE_HJID) ");
        sb2.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb2.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb2.append(" where ( ft.TYPE_='splice variant') ");
        Dataset<Row> df2 = DBUtils.executeSQLsourceUniprot("(" + sb2.toString() + ") as tbl");

        Dataset<Row> df = df1.join(df2, df1.col(CommonConstants.COL_FEATURE_ID).equalTo(df2.col(CommonConstants.COL_FEATURE_ID)), "inner")
                .drop(df2.col(CommonConstants.COL_FEATURE_ID));

        return df;
    }

    public static Dataset<Row> getSequenceVariationInRange(String featureId)
    {
        StringBuffer sb = new StringBuffer();

        sb.append("select ");
        sb.append(CommonConstants.COL_ORIGINAL + ", ");
        sb.append(CommonConstants.COL_VARIATION + ", ");
        sb.append(CommonConstants.COL_BEGIN + ", ");
        sb.append(CommonConstants.COL_END + " ");
        sb.append("from ");
        sb.append("( select ftv.HJVALUE as "+ CommonConstants.COL_VARIATION + ", ");
        sb.append("ft.ORIGINAL as "+ CommonConstants.COL_ORIGINAL + ", ");
        sb.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_BEGIN + " ");
        sb.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.BEGIN__LOCATION_TYPE_HJID) ");
        sb.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb.append("where ( ft.ID='"+featureId+"') ) AS tbl1 ");
        sb.append("LEFT OUTER JOIN ");
        sb.append("( select ftv.HJVALUE as "+ CommonConstants.COL_VARIATION + "2, ");
        sb.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_END + " ");
        sb.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.END__LOCATION_TYPE_HJID) ");
        sb.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb.append(" where ( ft.ID='"+featureId+"') ) AS tbl2 ");
        sb.append(" ON tbl1."+CommonConstants.COL_VARIATION+" = tbl2."+CommonConstants.COL_VARIATION+"2 ");
        sb.append("or tbl1."+CommonConstants.COL_VARIATION+" is NULL and tbl2."+CommonConstants.COL_VARIATION+"2 is NULL ");

        Dataset<Row> df = DBUtils.executeSQLsourceUniprot("(" + sb.toString() + ") as tbl");
        return df;
    }

    public static Dataset<Row> getSinglePositionVariations()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("select ft.ID as "+ CommonConstants.COL_FEATURE_ID + ", ");
        sb.append("ftv.HJVALUE as "+ CommonConstants.COL_VARIATION + ", ");
        sb.append("ft.ORIGINAL as "+ CommonConstants.COL_ORIGINAL + ", ");
        sb.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_POSITION + " ");
        sb.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.POSITION__LOCATION_TYPE_HJID) ");
        sb.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb.append(" where ( ft.TYPE_='splice variant') ");

        Dataset<Row> df = DBUtils.executeSQLsourceUniprot("(" + sb.toString() + ") as tbl");
        return df;
    }

    public static Dataset<Row> getSequenceVariationAtPosition(String featureId)
    {
        StringBuffer sb = new StringBuffer();

        sb.append("select ftv.HJVALUE as "+ CommonConstants.COL_VARIATION + ", ");
        sb.append("ft.ORIGINAL as "+ CommonConstants.COL_ORIGINAL + ", ");
        sb.append("CAST(pt.POSITION_ AS SIGNED) as " + CommonConstants.COL_POSITION + " ");
        sb.append("from position_type AS pt JOIN location_type AS lt ON (pt.HJID=lt.POSITION__LOCATION_TYPE_HJID) ");
        sb.append("JOIN feature_type AS ft ON (ft.LOCATION__FEATURE_TYPE_HJID=lt.HJID) ");
        sb.append("LEFT OUTER JOIN feature_type_variation AS ftv ON (ft.HJID=ftv.HJID) ");
        sb.append(" where ( ft.ID='"+featureId+"') ");


        Dataset<Row> df = DBUtils.executeSQLsourceUniprot("(" + sb.toString() + ") as tbl");
        return df;
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

    public static void main(String[] args)
    {
        String var = "VSP_057275";
        Dataset<Row> df = getGeneNameToUniProtAccessionsMap(9606);
        df.filter(col(CommonConstants.GENE_NAME).equalTo("ACAN")).show();

        //df.filter(col(CommonConstants.COL_UNIPROT_ACCESSION).equalTo("Q8NA29")).show();

    }
}
