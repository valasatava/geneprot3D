package org.rcsb.geneprot.genomemapping.constants;

import java.util.regex.Pattern;

/**
 * Created by Yana Valasatava on 9/29/17.
 */
public class CommonConstants {

    public static final String HUMAN_GENOME_ASSEMBLY_GRCH37 = "hg19";
    public static final String HUMAN_GENOME_ASSEMBLY_GRCH38 = "hg38";
    public static final String MOUSE_GENOME_ASSEMBLY_GRCH38 = "mm10";

    public static final String COL_NCBI_RNA_SEQUENCE_ACCESSION = "rnaSequenceIdentifier";
    public static final String COL_NCBI_PROTEIN_SEQUENCE_ACCESSION = "proteinSequenceIdentifier";

    public static final String COL_ID = "id";

    public static final String COL_CHROMOSOME = "chromosome";

    public static final String COL_GENE_NAME = "geneName";
    public static final String COL_GENE_ID = "geneId";

    public static final String COL_ORIENTATION = "orientation";

    public static final String COL_TX_START = "transcriptionStart";
    public static final String COL_TX_END = "transcriptionEnd";
    public static final String COL_CDS_START = "cdsStart";
    public static final String COL_CDS_END = "cdsEnd";
    public static final String COL_EXONS_COUNT = "exonsCount";
    public static final String COL_EXONS_START = "exonsStart";
    public static final String COL_EXONS_END = "exonsEnd";

    public static final String COL_TRANSCRIPT = "transcript";
    public static final String COL_TRANSCRIPT_NAME = "transcriptName";
    public static final String COL_TRANSCRIPT_ID = "transcriptId";
    public static final String COL_TRANSCRIPTS = "isoforms";

    public static final String COL_TRANSCRIPTION = "transcription";
    public static final String COL_UNTRANSLATED = "untranslated";
    public static final String COL_CODING = "cds";

    public static final String COL_UTR = "utr";
    public static final String COL_UTRS = "utrs";
    public static final String COL_UTR3 = "utr3";
    public static final String COL_UTR5 = "utr5";

    public static final String COL_EXON = "exon";
    public static final String COL_EXON_ID = "exonId";
    public static final String COL_EXON_NUMBER = "exonNumber";
    public static final String COL_EXONS = "exons";

    public static final String COL_CCDS_ID = "ccdsId";

    public static final String COL_START_CODON = "startCodon";
    public static final String COL_STOP_CODON = "stopCodon";

    public static final String COL_UNIPROT_ACCESSION = "uniProtId";
    public static final String COL_MOLECULES = "molecules";
    public static final String COL_MOLECULE_ID = "moleculeId";
    public static final String COL_ISOFORM_ID = "isoformId";

    public static final String COL_ISOFORMS = "isoforms";
    public static final String COL_COORDINATES_ISOFORM = "isoformCoordinates";

    public static final String COL_MATCH = "match";
    public static final String COL_MAPPING = "mapping";
    public static final String COL_MRNA_MAPPING = "mRNAMapping";
    public static final String COL_PROTEIN_MAPPING = "proteinMapping";

    public static final String COL_ALTERNATIVE_EXONS = "alternativeExons";
    public static final String COL_HAS_ALTERNATIVE_EXONS = "hasAlternativeExons";

    public static final String COL_SEQUENCE = "sequence";
    public static final String COL_SEQUENCE_STATUS = "sequenceStatus";
    public static final String COL_PROTEIN_SEQUENCE = "proteinSequence";
    public static final String COL_SEQUENCE_TYPE = "sequenceType";
    public static final String COL_FEATURE_ID = "featureId";
    public static final String COL_FEATURE_TYPE = "featureType";
    public static final String COL_FEATURES="features";

    public static final String COL_ORIGINAL = "original";
    public static final String COL_VARIATION = "variation";
    public static final String COL_BEGIN = "begin";
    public static final String COL_START = "start";
    public static final String COL_END = "end";

    public static final String COL_START_UNP = "startUNP";
    public static final String COL_END_UNP = "endUNP";
    public static final String COL_START_PDB = "startPDB";
    public static final String COL_END_PDB = "endPDB";

    public static final String COL_SINGLE_AMINO_ACID = "singleAminoAcid";
    public static final String COL_SINGLE_AMINO_ACID_VARIATION = "singleAminoAcidVariation";
    public static final String COL_POSITION = "position";

    public static final String KEY_SEPARATOR = "=";
    public static final String DASH = Pattern.quote("-");
    public static final String DOT = Pattern.quote(".");
    public static final String FIELD_SEPARATOR = Pattern.quote("\t");
    public static final String EXONS_FIELD_SEPARATOR = Pattern.quote(",");

    public static final String COL_ENTRY_ID = "entryId";
    public static final String COL_ENTITY_ID = "entityId";
    public static final String COL_CHAIN_ID = "chainId";

    public static final String COL_CODING_COORDINATES = "codingCoordinates";
    public static final String COL_ISOFORM_COORDINATES = "isoformCoordinates";
    public static final String COL_STRUCTURE_COORDINATES = "structureCoordinates";
}
