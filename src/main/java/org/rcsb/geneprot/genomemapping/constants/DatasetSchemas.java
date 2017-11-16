package org.rcsb.geneprot.genomemapping.constants;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class DatasetSchemas {

    public static final StructType GENOME_ANNOTATION_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(CommonConstants.COL_GENE_NAME, DataTypes.StringType, false)
                    , DataTypes.createStructField(CommonConstants.COL_NCBI_RNA_SEQUENCE_ACCESSION, DataTypes.StringType, false)
                    , DataTypes.createStructField(CommonConstants.COL_CHROMOSOME, DataTypes.StringType, false)
                    , DataTypes.createStructField(CommonConstants.COL_ORIENTATION, DataTypes.StringType, false)
                    , DataTypes.createStructField(CommonConstants.COL_TX_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_TX_END, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_CDS_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_CDS_END, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_EXONS_COUNT, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_EXONS_START, DataTypes.createArrayType(DataTypes.IntegerType), false)
                    , DataTypes.createStructField(CommonConstants.COL_EXONS_END, DataTypes.createArrayType(DataTypes.IntegerType), false)
            });
    public static final StructType RANGE_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(CommonConstants.COL_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_END, DataTypes.IntegerType, false)
            });
    public static final StructType RANGE_SCHEMA_WITH_ID = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(CommonConstants.COL_ID, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(CommonConstants.COL_END, DataTypes.IntegerType, false)
            });
    public static final StructType GENCODE_TRANSCRIPT_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(CommonConstants.COL_CHROMOSOME, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_GENE_NAME, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_GENE_ID, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_ORIENTATION, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_TRANSCRIPT_NAME, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_TRANSCRIPT_ID, DataTypes.StringType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_TRANSCRIPTION, RANGE_SCHEMA, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_START_CODON, RANGE_SCHEMA, true, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_STOP_CODON, RANGE_SCHEMA, true, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_UTR, DataTypes.createArrayType(RANGE_SCHEMA), false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_CCDS_ID, DataTypes.StringType, true, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_CODING, DataTypes.createArrayType(RANGE_SCHEMA_WITH_ID), false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_EXONS_COUNT, DataTypes.IntegerType, false, Metadata.empty())
                    , DataTypes.createStructField(CommonConstants.COL_EXONS, DataTypes.createArrayType(RANGE_SCHEMA_WITH_ID), false, Metadata.empty())
            });
    public static final StructType UNIPROT_TO_TRANSCRIPT_SCHEMA = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField(CommonConstants.COL_UNIPROT_ACCESSION, DataTypes.StringType, true)
                    , DataTypes.createStructField(CommonConstants.COL_TRANSCRIPT_ID, DataTypes.StringType, true)
            });
}
