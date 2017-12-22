package org.rcsb.geneprot.genomemapping.functions;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.mojave.genomemapping.SequenceToStructureFeaturesMap;
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
public class MapStructureToProteinIsoformsCoordinates implements FlatMapFunction<Row, SequenceToStructureFeaturesMap> {

    private static final Logger logger = LoggerFactory.getLogger(MapStructureToProteinIsoformsCoordinates.class);

    public static String getMoleculeId(String id) {
        if (!id.contains("-"))
            id += "-1";
        return id;
    }

    public static String getUniProtId(String moleculeId) {
        return moleculeId.split("-")[0];
    }

    public static Iterator<SequenceToStructureFeaturesMap> getCoordinatesForAllIsoforms(String entryId) throws Exception {

        HttpResponse<JsonNode> response = Unirest
                .get("https://www.ebi.ac.uk/pdbe/api/mappings/all_isoforms/{id}")
                .routeParam("id", entryId)
                .asJson();

        if (response.getStatus() != 200)
            return new ArrayList<SequenceToStructureFeaturesMap>().iterator();

        JsonNode body = response.getBody();
        JSONObject obj = body.getObject()
                    .getJSONObject(entryId.toLowerCase())
                    .getJSONObject("UniProt");

        Map<String, SequenceToStructureFeaturesMap> map = new HashMap<>();

        Iterator<String> it = obj.keys();
        while (it.hasNext()) {

            String id = it.next();
            String moleculeId = getMoleculeId(id);
            String uniProtId = getUniProtId(moleculeId);

            JSONArray mappings = obj.getJSONObject(id).getJSONArray("mappings");
            for (int j=0; j < mappings.length();j++) {

                JSONObject m = mappings.getJSONObject(j);
                int entityId = m.getInt("entity_id");
                String chainId = m.getString("chain_id");

                String key = entryId + CommonConstants.KEY_SEPARATOR + chainId + CommonConstants.KEY_SEPARATOR + moleculeId;

                if ( !map.keySet().contains(key) ) {
                    SequenceToStructureFeaturesMap mapper = new SequenceToStructureFeaturesMap();
                    mapper.setEntryId(entryId);
                    mapper.setEntityId(Integer.toString(entityId));
                    mapper.setChainId(chainId);
                    mapper.setUniProtId(uniProtId);
                    mapper.setMoleculeId(moleculeId);
                    mapper.setCoordinates(new ArrayList<>());
                    map.put(key, mapper);
                }

                SegmentMapping segment = new SegmentMapping();
                segment.setId(map.get(key).getCoordinates().size()+1);

                PositionMapping start = new PositionMapping();
                start.setUniProtPosition(m.getInt("unp_start"));
                start.setPdbSeqPosition(m.getInt("pdb_start"));
                segment.setStart(start);

                PositionMapping end = new PositionMapping();
                end.setUniProtPosition(m.getInt("unp_end"));
                end.setPdbSeqPosition(m.getInt("pdb_end"));
                segment.setEnd(end);

                map.get(key).getCoordinates().add(segment);
            }
        }
        return map.values().iterator();
    }

    @Override
    public Iterator<SequenceToStructureFeaturesMap> call(Row row) throws Exception {

        String entryId = row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID));
        logger.info("Getting isoforms mapping for {}", entryId);
        return getCoordinatesForAllIsoforms(entryId);
    }
}