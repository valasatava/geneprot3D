package org.rcsb.geneprot.common.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.regex.Pattern;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class CommonConstants {

    public static final String HUMAN_GENOME_ASSEMBLY_GRCH38 = "hg38";
    public static final String MOUSE_GENOME_ASSEMBLY_GRCH38 = "mm10";

    public static final String HUMAN_GENOME_ASSEMBLY_GRCH37 = "hg19";

    public static final String COL_NCBI_RNA_SEQUENCE_ACCESSION = "rnaSequenceIdentifier";
    public static final String COL_NCBI_PROTEIN_SEQUENCE_ACCESSION = "proteinSequenceIdentifier";

    public static final String COL_CHROMOSOME = "chromosome";
    public static final String COL_GENE_NAME = "geneName";
    public static final String COL_ORIENTATION = "orientation";
    public static final String COL_TX_START = "transcriptionStart";
    public static final String COL_TX_END = "transcriptionEnd";
    public static final String COL_CDS_START = "cdsStart";
    public static final String COL_CDS_END = "cdsEnd";
    public static final String COL_EXONS_COUNT = "exonsCount";
    public static final String COL_EXONS_START = "exonsStart";
    public static final String COL_EXONS_END = "exonsEnd";

    public static final String COL_TRANSCRIPT = "transcript";
    public static final String COL_TRANSCRIPTS = "transcripts";

    public static final String COL_UNIPROT_ACCESSION = "uniProtId";
    public static final String COL_MOLECULES = "molecules";
    public static final String COL_MOLECULE_ID = "moleculeId";
    public static final String COL_ISOFORM_ID = "isoformId";

    public static final String COL_MATCH = "match";
    public static final String COL_MAPPING = "mapping";
    public static final String COL_MRNA_MAPPING = "mRNAMapping";
    public static final String COL_PROTEIN_MAPPING = "proteinMapping";

    public static final String COL_ALTERNATIVE_EXONS = "alternativeExons";
    public static final String COL_HAS_ALTERNATIVE_EXONS = "hasAlternativeExons";

    public static final String COL_PROTEIN_SEQUENCE = "proteinSequence";
    public static final String COL_SEQUENCE_TYPE = "sequenceType";
    public static final String COL_FEATURE_ID = "featureId";
    public static final String COL_FEATURE_TYPE = "featureType";
    public static final String COL_FEATURES="features";

    public static final String COL_ORIGINAL = "original";
    public static final String COL_VARIATION = "variation";
    public static final String COL_BEGIN = "begin";
    public static final String COL_END = "end";

    public static final String COL_SINGLE_AMINO_ACID = "singleAminoAcid";
    public static final String COL_SINGLE_AMINO_ACID_VARIATION = "singleAminoAcidVariation";
    public static final String COL_POSITION = "position";

    public static final String KEY_SEPARATOR = "=";
    public static final String DASH = Pattern.quote("-");
    public static final String DOT = Pattern.quote(".");
    public static final String FIELD_SEPARATOR = Pattern.quote("\t");
    public static final String EXONS_FIELD_SEPARATOR = Pattern.quote(",");

    public static final StructType GENOME_ANNOTATION_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(COL_GENE_NAME, DataTypes.StringType, false)
                    , DataTypes.createStructField(COL_NCBI_RNA_SEQUENCE_ACCESSION, DataTypes.StringType, false)
                    , DataTypes.createStructField(COL_CHROMOSOME, DataTypes.StringType, false)
                    , DataTypes.createStructField(COL_ORIENTATION, DataTypes.StringType, false)
                    , DataTypes.createStructField(COL_TX_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(COL_TX_END, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(COL_CDS_START, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(COL_CDS_END, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(COL_EXONS_COUNT, DataTypes.IntegerType, false)
                    , DataTypes.createStructField(COL_EXONS_START, DataTypes.createArrayType(DataTypes.IntegerType), false)
                    , DataTypes.createStructField(COL_EXONS_END, DataTypes.createArrayType(DataTypes.IntegerType), false)
            });

    public static final StructType NCBI_RNA_TO_PROTEIN_ACCESSION_SCHEMA = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField(COL_NCBI_RNA_SEQUENCE_ACCESSION, DataTypes.StringType, true)
                    , DataTypes.createStructField(COL_NCBI_PROTEIN_SEQUENCE_ACCESSION, DataTypes.StringType, true)
            });

    public static final StructType NCBI_PROTEIN_TO_UNIPROT_ACCESSION_SCHEMA = DataTypes
            .createStructType(new StructField[]{
                      DataTypes.createStructField(COL_NCBI_PROTEIN_SEQUENCE_ACCESSION, DataTypes.StringType, true)
                    , DataTypes.createStructField(org.rcsb.mojave.util.CommonConstants.COL_UNIPROT_ACCESSION, DataTypes.StringType, true)
            });

    public static final StructType TRANSCRIPTS_SCHEMA = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField(COL_TRANSCRIPTS, DataTypes.createArrayType(
                            DataTypes
                                    .createStructType(new StructField[]{
                                              DataTypes.createStructField(COL_NCBI_RNA_SEQUENCE_ACCESSION, DataTypes.StringType, false)
                                            , DataTypes.createStructField(COL_NCBI_PROTEIN_SEQUENCE_ACCESSION, DataTypes.StringType, false)
                                            , DataTypes.createStructField(COL_MOLECULE_ID, DataTypes.StringType, false)
                                            , DataTypes.createStructField(COL_ISOFORM_ID, DataTypes.StringType, false)
                                            , DataTypes.createStructField(COL_TX_START, DataTypes.IntegerType, false, Metadata.empty())
                                            , DataTypes.createStructField(COL_TX_END, DataTypes.IntegerType, false, Metadata.empty())
                                            , DataTypes.createStructField(COL_CDS_START, DataTypes.IntegerType, false, Metadata.empty())
                                            , DataTypes.createStructField(COL_CDS_END, DataTypes.IntegerType, false, Metadata.empty())
                                            , DataTypes.createStructField(COL_EXONS_COUNT, DataTypes.IntegerType, false, Metadata.empty())
                                            , DataTypes.createStructField(COL_EXONS_START, DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty())
                                            , DataTypes.createStructField(COL_EXONS_END, DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty())
                                    })
                    ), true)
            });

    public static final StructType MAPPING_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                      DataTypes.createStructField(COL_MOLECULE_ID, DataTypes.StringType, true, Metadata.empty())
                    , DataTypes.createStructField(COL_MRNA_MAPPING, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)), true, Metadata.empty())
                    , DataTypes.createStructField(COL_PROTEIN_MAPPING, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)), true, Metadata.empty())
            });

    public static final StructType GENOME_MAPPING_SCHEMA = DataTypes
            .createStructType(new StructField[] {
                  DataTypes.createStructField(COL_CHROMOSOME, DataTypes.StringType, false, Metadata.empty())
                , DataTypes.createStructField(COL_GENE_NAME, DataTypes.StringType, false, Metadata.empty())
                , DataTypes.createStructField(COL_ORIENTATION, DataTypes.StringType, false, Metadata.empty())
                , DataTypes.createStructField(COL_UNIPROT_ACCESSION, DataTypes.StringType, false, Metadata.empty())
                //, DataTypes.createStructField(COL_TRANSCRIPTS, TRANSCRIPTS_SCHEMA, false, Metadata.empty())
                , DataTypes.createStructField(COL_MAPPING, MAPPING_SCHEMA, false, Metadata.empty())
    });
}
