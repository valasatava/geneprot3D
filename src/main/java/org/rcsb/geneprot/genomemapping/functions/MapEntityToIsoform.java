package org.rcsb.geneprot.genomemapping.functions;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.CoordinatesRange;
import org.rcsb.geneprot.genomemapping.model.EntityToIsoform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class MapEntityToIsoform implements FlatMapFunction<Row, EntityToIsoform> {

    private static final Logger logger = LoggerFactory.getLogger(MapEntityToIsoform.class);

    public static Iterator<EntityToIsoform> getIsoformsCoordinates(String entryId) throws Exception {

        HttpResponse<JsonNode> response = Unirest.get("https://www.ebi.ac.uk/pdbe/api/mappings/all_isoforms/{id}")
                .routeParam("id", entryId).asJson();

        if (response.getStatus() != 200)
            return new ArrayList<EntityToIsoform>().iterator();

        JsonNode body = response.getBody();
        JSONObject obj = body.getObject()
                    .getJSONObject(entryId.toLowerCase())
                    .getJSONObject("UniProt");
        
        Iterator<String> it = obj.keys();
        Map<String, EntityToIsoform> map = new HashMap<>();
        while (it.hasNext()) {

            String id = it.next();
            JSONArray mappings = obj.getJSONObject(id).getJSONArray("mappings");

            String moleculeId = id;
            if (!moleculeId.contains("-"))
                moleculeId += "-1";
            String uniProtId = moleculeId.split("-")[0];

            for (int j=0; j < mappings.length();j++) {

                JSONObject m = mappings.getJSONObject(j);
                int entityId = m.getInt("entity_id");
                String chainId = m.getString("chain_id");
                String key = entryId + CommonConstants.KEY_SEPARATOR + chainId + CommonConstants.KEY_SEPARATOR + moleculeId;
                if (!map.keySet().contains(key)) {
                    EntityToIsoform isoform = new EntityToIsoform();
                    isoform.setEntryId(entryId);
                    isoform.setEntityId(Integer.toString(entityId));
                    isoform.setChainId(chainId);
                    isoform.setUniProtId(uniProtId);
                    isoform.setMoleculeId(moleculeId);
                    map.put(key, isoform);
                }
                map.get(key).getIsoformCoordinates()
                        .add(new CoordinatesRange(m.getInt("unp_start"), m.getInt("unp_end")));
                map.get(key).getStructureCoordinates()
                        .add(new CoordinatesRange(m.getInt("pdb_start"), m.getInt("pdb_end")));
            }
        }
        return map.values().iterator();
    }

    @Override
    public Iterator<EntityToIsoform> call(Row row) throws Exception {

        String entryId = row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID));
        logger.info("Getting isoforms mapping for {}", entryId);
        return getIsoformsCoordinates(entryId);
    }
}
