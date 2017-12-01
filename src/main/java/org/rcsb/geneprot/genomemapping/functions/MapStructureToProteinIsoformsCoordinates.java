package org.rcsb.geneprot.genomemapping.functions;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.mojave.genomemapping.mappers.StructureSeqLocationMap;
import org.rcsb.mojave.mappers.PositionMapping;
import org.rcsb.mojave.mappers.SegmentMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class MapStructureToProteinIsoformsCoordinates implements FlatMapFunction<Row, StructureSeqLocationMap> {

    private static final Logger logger = LoggerFactory.getLogger(MapStructureToProteinIsoformsCoordinates.class);

    public static Iterator<StructureSeqLocationMap> getCoordinatesForAllIsoforms(String entryId) throws Exception {

        HttpResponse<JsonNode> response = Unirest
                .get("https://www.ebi.ac.uk/pdbe/api/mappings/all_isoforms/{id}")
                .routeParam("id", entryId)
                .asJson();

        if (response.getStatus() != 200)
            return new ArrayList<StructureSeqLocationMap>().iterator();

        JsonNode body = response.getBody();
        JSONObject obj = body.getObject()
                    .getJSONObject(entryId.toLowerCase())
                    .getJSONObject("UniProt");
        
        Iterator<String> it = obj.keys();
        Map<String, StructureSeqLocationMap> mapStructSeqLocation = new HashMap<>();
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

                if (!mapStructSeqLocation.keySet().contains(key)) {
                    StructureSeqLocationMap map = new StructureSeqLocationMap();
                    map.setEntryId(entryId);
                    map.setEntityId(Integer.toString(entityId));
                    map.setChainId(chainId);
                    map.setUniProtId(uniProtId);
                    map.setMoleculeId(moleculeId);
                    map.setSequenceLocation(new ArrayList<>());
                    mapStructSeqLocation.put(key, map);
                }
                SegmentMapping segment = new SegmentMapping();
                segment.setId(j+1);

                PositionMapping start = new PositionMapping();
                start.setUniProtResNum(m.getInt("unp_start"));
                start.setPdbSeqResNum(m.getInt("pdb_start"));
                segment.setStart(start);

                PositionMapping end = new PositionMapping();
                end.setUniProtResNum(m.getInt("unp_end"));
                end.setPdbSeqResNum(m.getInt("pdb_end"));
                segment.setEnd(end);

                mapStructSeqLocation.get(key)
                        .getSequenceLocation().add(segment);
            }
        }
        return mapStructSeqLocation.values().iterator();
    }

    @Override
    public Iterator<StructureSeqLocationMap> call(Row row) throws Exception {

        String entryId = row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID));
        logger.info("Getting isoforms mapping for {}", entryId);
        return getCoordinatesForAllIsoforms(entryId);
    }
}
