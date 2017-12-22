package org.rcsb.geneprot.common.tools;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.rcsb.mojave.util.MongoCollections;
import org.rcsb.redwood.util.StoichiometryRegEx;

import java.util.ArrayList;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;

/**
 * Created by Yana Valasatava on 12/7/17.
 */
public class UpdateMongoCollection {

    public static void updateStoichiometry() {

        String MONGO_DB_IP = "132.249.213.154";
        String MONGO_DB_NAME = "dw_v1";
        String MONGO_COLL_NAME = MongoCollections.COLL_PROTEIN_SYMMETRY;
        MongoClient client = new MongoClient(MONGO_DB_IP);

        MongoCollection<Document> collection = client.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLL_NAME);
        MongoCursor<Document> it = collection.find().iterator();
        while (it.hasNext()){

            Document document = it.next();

            Document query = new Document("_id", document.get("_id"));

            Document fields = new Document();

            // GLOBAL SYMMETRY
            if (document.containsKey("globalSymmetry")) {
                Document symmetry = document.get("globalSymmetry", Document.class);
                String stoichiometry = symmetry.getString("stoichiometry");
                String description = StoichiometryRegEx.getOligomericComposition(stoichiometry);
                collection.updateOne(eq("_id", document.get("_id"))
                        , set("globalSymmetry.stoichiometryDescription", description));
            }

            // PSEUDO SYMMETRY
            if (document.containsKey("pseudoSymmetry")) {
                Document symmetry = document.get("pseudoSymmetry", Document.class);
                String stoichiometry = symmetry.getString("stoichiometry");
                String description = StoichiometryRegEx.getOligomericComposition(stoichiometry);
                collection.updateOne(eq("_id", document.get("_id"))
                        , set("pseudoSymmetry.stoichiometryDescription", description));
            }

            // LOCAL SYMMETRY
            if (document.containsKey("localSymmetry")) {
                ArrayList symmetryArray = document.get("localSymmetry", ArrayList.class);
                for ( int i=0; i < symmetryArray.size(); i++ ){
                    Document symmetry = (Document)symmetryArray.get(i);
                    String stoichiometry = symmetry.getString("stoichiometry");
                    String description = StoichiometryRegEx.getOligomericComposition(stoichiometry);
                    collection.updateOne(eq("_id", document.get("_id"))
                            , set("localSymmetry."+i+".stoichiometryDescription", description));
                }
            }
        }
    }

    public static void updateLongStoichiometryNames() {

        String MONGO_DB_IP = "132.249.213.154";
        String MONGO_DB_NAME = "dw_v1";
        String MONGO_COLL_NAME = MongoCollections.COLL_PROTEIN_SYMMETRY;

        MongoClient client = new MongoClient(MONGO_DB_IP);

        MongoCollection<Document> collection = client.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLL_NAME);
        MongoCursor<Document> it = collection.find().iterator();
        while (it.hasNext()){

            Document document = it.next();

            Document query = new Document("_id", document.get("_id"));

            Document fields = new Document();

            // GLOBAL SYMMETRY
            if (document.containsKey("globalSymmetry")) {
                Document symmetry = document.get("globalSymmetry", Document.class);
                String stoichiometry = symmetry.getString("stoichiometry");
                if (stoichiometry.contains("?"))
                    collection.updateOne(eq("_id", document.get("_id"))
                        , unset("globalSymmetry.stoichiometry"));
            }

            // PSEUDO SYMMETRY
            if (document.containsKey("pseudoSymmetry")) {
                Document symmetry = document.get("pseudoSymmetry", Document.class);
                String stoichiometry = symmetry.getString("stoichiometry");
                if (stoichiometry.contains("?"))
                    collection.updateOne(eq("_id", document.get("_id"))
                            , unset("pseudoSymmetry.stoichiometry"));
            }

            // LOCAL SYMMETRY
            if (document.containsKey("localSymmetry")) {
                ArrayList symmetryArray = document.get("localSymmetry", ArrayList.class);
                for ( int i=0; i < symmetryArray.size(); i++ ){
                    Document symmetry = (Document)symmetryArray.get(i);
                    String stoichiometry = symmetry.getString("stoichiometry");
                    if (stoichiometry.contains("?"))
                        collection.updateOne(eq("_id", document.get("_id"))
                                , unset("localSymmetry."+i+".stoichiometry"));
                }
            }
        }
    }

    public static void main(String[] args) {
        updateLongStoichiometryNames();
    }
}
