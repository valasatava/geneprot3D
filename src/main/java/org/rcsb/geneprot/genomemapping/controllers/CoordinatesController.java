package org.rcsb.geneprot.genomemapping.controllers;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.constants.MongoCollections;
import org.rcsb.geneprot.genomemapping.models.GenomicPosition;
import org.rcsb.geneprot.genomemapping.models.IsoformPosition;
import org.rcsb.geneprot.genomemapping.models.Position;
import org.rcsb.geneprot.genomemapping.models.StructurePosition;
import org.rcsb.redwood.util.DBConnectionUtils;
import org.rcsb.redwood.util.DBUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Yana Valasatava on 11/27/17.
 */
public class CoordinatesController {

    private static SparkSession sparkSession = SparkUtils.getSparkSession();
    private static Map<String, String> mongoDBOptions = DBConnectionUtils.getMongoDBOptions();

    public static int taxonomyId;

    public static int getTaxonomyId() {
        return taxonomyId;
    }

    public static void setTaxonomyId(int taxonomyId) {
        CoordinatesController.taxonomyId = taxonomyId;
    }

    public static List<Position> mapGeneticPositionToSequence(String chromosome, int position) {

        MongoCollection<Document> collection = DBUtils.getMongoCollection(MongoCollections.COLL_MAPPING_TRANSCRIPTS_TO_ISOFORMS + "_" + getTaxonomyId());

        List<Document> query = Arrays.asList(
                 new Document("$match", new Document("$or", Arrays.asList(
                                new Document("$and", Arrays.asList(
                                          new Document(CommonConstants.COL_CHROMOSOME, new Document("$eq", chromosome))
                                        , new Document(CommonConstants.COL_ORIENTATION, new Document("$eq", "+"))
                                        , new Document(CommonConstants.COL_ISOFORMS+"."+ CommonConstants.COL_CODING_COORDINATES+"."+ CommonConstants.COL_START, new Document("$gte", position))
                                        , new Document(CommonConstants.COL_ISOFORMS+"."+ CommonConstants.COL_CODING_COORDINATES+"."+ CommonConstants.COL_END, new Document("$lte", position))))
                                , new Document("$and", Arrays.asList(
                                        new Document(CommonConstants.COL_CHROMOSOME, new Document("$eq", chromosome))
                                        , new Document(CommonConstants.COL_ORIENTATION, new Document("$eq", "-"))
                                        , new Document(CommonConstants.COL_ISOFORMS+"."+ CommonConstants.COL_CODING_COORDINATES+"."+ CommonConstants.COL_END, new Document("$gte", position))
                                        , new Document(CommonConstants.COL_ISOFORMS+"."+ CommonConstants.COL_CODING_COORDINATES+"."+ CommonConstants.COL_START, new Document("$lte", position)))))))
                , new Document("$unwind", new Document("path", "$"+CommonConstants.COL_ISOFORMS)));

        AggregateIterable<Document> output = collection.aggregate(query);

        List<Position> results = new ArrayList<>();

        for (Document document : output) {

            int cdsId = 0;
            int delta = 0;
            ArrayList<Document> cds = document.get(CommonConstants.COL_ISOFORMS, Document.class).get(CommonConstants.COL_CODING_COORDINATES, ArrayList.class);
            for (Document c : cds) {

                Integer start = c.getInteger(CommonConstants.COL_START);
                Integer end = c.getInteger(CommonConstants.COL_END);

                if ((start<=position) && (position<=end)) {
                    if (document.getString(CommonConstants.COL_ORIENTATION).equals("-")) {
                        delta = end - position + 1;
                        cdsId = c.getInteger(CommonConstants.COL_ID);
                        break;
                    } else if (document.getString(CommonConstants.COL_ORIENTATION).equals("+")) {
                        delta = position - start;
                        cdsId = c.getInteger(CommonConstants.COL_ID);
                        break;
                    }
                }
            }

            int seqCoord = 0;
            char letter = '\0';
            ArrayList<Document> seq = document.get(CommonConstants.COL_ISOFORMS, Document.class).get(CommonConstants.COL_ISOFORM_COORDINATES, ArrayList.class);
            for (Document s : seq) {
                if (s.getInteger(CommonConstants.COL_ID) != cdsId)
                    continue;
                Integer start = s.getInteger(CommonConstants.COL_START);
                seqCoord = start + (int) Math.ceil(delta / 3.0f) - 1;
                String sequence = document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_SEQUENCE);
                letter = sequence.charAt(seqCoord - 1);
                break;
            }

            GenomicPosition gp = new GenomicPosition();
            gp.setChromosome(document.getString(CommonConstants.COL_CHROMOSOME));
            gp.setOrientation(document.getString(CommonConstants.COL_ORIENTATION));
            gp.setGeneId(document.getString(CommonConstants.COL_GENE_ID));
            gp.setGeneName(document.getString(CommonConstants.COL_GENE_NAME));
            gp.setTranscriptId(document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_TRANSCRIPT_ID));
            gp.setTranscriptName(document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_TRANSCRIPT_NAME));
            gp.setCoordinate(position);

            IsoformPosition ip = new IsoformPosition();
            ip.setUniProtId(document.getString(CommonConstants.COL_UNIPROT_ACCESSION));
            ip.setMoleculeId(document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_MOLECULE_ID));
            ip.setCoordinate(seqCoord);
            ip.setLetter(letter);

            Position p = new Position();
            p.setGenomicPosition(gp);
            p.setIsoformPosition(ip);
            results.add(p);

            System.out.println(document.getString(CommonConstants.COL_CHROMOSOME)
                    +" "+document.getString(CommonConstants.COL_GENE_NAME)
                    +" "+document.getString(CommonConstants.COL_ORIENTATION)
                    +" "+document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_TRANSCRIPT_ID)
                    +" "+position
                    +" -> "
                    +" "+document.getString(CommonConstants.COL_UNIPROT_ACCESSION)
                    +" "+document.get(CommonConstants.COL_ISOFORMS, Document.class).getString(CommonConstants.COL_MOLECULE_ID)
                    +" "+seqCoord+" "+letter);
        }
        System.out.println("\n");
        return results;
    }

    public static List<Position> mapSequencePositionToStructure(String uniProtId, int position) {

        MongoCollection<Document> collection = DBUtils.getMongoCollection(MongoCollections.COLL_MAPPING_ENTITIES_TO_ISOFORMS + "_" + getTaxonomyId());

        List<Document> query = Arrays.asList(
                new Document("$match", new Document("$and", Arrays.asList(
                          new Document(CommonConstants.COL_UNIPROT_ACCESSION, new Document("$eq", uniProtId))
                        , new Document(CommonConstants.COL_ISOFORM_COORDINATES, new Document("$elemMatch"
                            , new Document("$and", Arrays.asList(
                                  new Document(CommonConstants.COL_START, new Document("$lte", position))
                                , new Document(CommonConstants.COL_END, new Document("$gte", position))))))))));

        List<Position> results = new ArrayList<>();

        AggregateIterable<Document> output = collection.aggregate(query);
        for (Document document : output) {

            int structCoord = 0;
            char letter = '\0';
            ArrayList<Document> seqCoords = document.get(CommonConstants.COL_ISOFORM_COORDINATES, ArrayList.class);
            ArrayList<Document> structCoords = document.get(CommonConstants.COL_STRUCTURE_COORDINATES, ArrayList.class);
            for (int i = 0; i < seqCoords.size(); i++) {
                Integer startSeq = seqCoords.get(i).getInteger(CommonConstants.COL_START);
                Integer endSeq = seqCoords.get(i).getInteger(CommonConstants.COL_END);
                if ( (startSeq<=position) && (position<=endSeq) ) {
                    int delta = position - startSeq;
                    structCoord = structCoords.get(i).getInteger(CommonConstants.COL_START) + delta;
                    break;
                }
            }

            StructurePosition sp = new StructurePosition();
            sp.setEntryId(document.getString(CommonConstants.COL_ENTRY_ID));
            sp.setEntityId(document.getString(CommonConstants.COL_ENTITY_ID));
            sp.setChainId(document.getString(CommonConstants.COL_CHAIN_ID));
            sp.setCoordinate(structCoord);

            Position p = new Position();
            p.setStructurePosition(sp);

            results.add(p);

            System.out.println(document.getString(CommonConstants.COL_UNIPROT_ACCESSION)
                    +" "+document.getString(CommonConstants.COL_MOLECULE_ID)
                    +" "+position+" -> "
                    +" "+document.getString(CommonConstants.COL_ENTRY_ID)
                    +" "+document.getString(CommonConstants.COL_CHAIN_ID)
                    +" "+structCoord);
        }

        return results;
    }

    public static void testGeneticToSeq() {

//        // VEGFB +
//        String chromosome = "chr11";
//        int position = 64237190; // P
//        //int position = 64237191; // K
//        //int position = 64237193; // K
//        //int position = 64237200; // D

        // HBB -
        String chromosome = "chr11";
        //int position = 5227002; // E
        //int position = 5227010; // L
        int position = 5226991; // A

//        // BIN1 "-"
//        String chromosome = "chr2";
//        int position = 127076640;

        mapGeneticPositionToSequence(chromosome, position);
    }

    public static void testSeqToStruct() {

        // HHB
        String uniProtId = "P68871";
        int position = 11;

        mapSequencePositionToStructure(uniProtId, position);
    }

    public static void main(String[] args) {

        String taxonomy = args[0];
        setTaxonomyId(Integer.valueOf(taxonomy));

        //testGeneticToSeq();

        testSeqToStruct();
    }
}