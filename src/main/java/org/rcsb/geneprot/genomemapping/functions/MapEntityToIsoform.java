package org.rcsb.geneprot.genomemapping.functions;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.rcsb.geneprot.genomemapping.constants.CommonConstants;
import org.rcsb.geneprot.genomemapping.model.EntityToIsoform;

import java.util.Iterator;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class MapEntityToIsoform implements Function<Row, EntityToIsoform> {

    public static JSONArray getIsoformsCoordinates(String entryId) throws Exception {

        HttpResponse<JsonNode> response = Unirest.get("https://www.ebi.ac.uk/pdbe/api/mappings/all_isoforms/{id}")
                .routeParam("id", entryId)
                .asJson();

        if (response.getStatus() != 200)
            return new JSONArray();

        JsonNode body = response.getBody();
        JSONObject obj = body.getObject()
                    .getJSONObject(entryId.toLowerCase())
                    .getJSONObject("UniProt");

        Iterator<String> it = obj.keys();
        while (it.hasNext()) {

        }

        return null;
    }

    @Override
    public EntityToIsoform call(Row row) throws Exception {

        String entryId = row.getString(row.fieldIndex(CommonConstants.COL_ENTRY_ID));


        JSONArray mapping = getIsoformsCoordinates(entryId);

//        RangeConverter converter = new RangeConverter();
//
//        List<Row> isoforms = row.getList(row.fieldIndex(CommonConstants.COL_ISOFORMS));
//        for (Row iRow : isoforms) {
//
//            EntityToIsoform iso = new EntityToIsoform();
//            iso.setMoleculeId(iRow.getString(iRow.fieldIndex(CommonConstants.COL_MOLECULE_ID)));
//
//            JSONArray map = mapping.get(iso.getMoleculeId());
//
//            List<Object> isoCoords = iRow.getList(iRow.fieldIndex(CommonConstants.COL_COORDINATES_ISOFORM));
//
//            for (int j=0; j<map.length(); j++) {
//
//                JSONObject range = map.getJSONObject(j);
//                converter.set(range.getInt("unp_start"), range.getInt("unp_end"), range.getInt("pdb_start"), range.getInt("pdb_end"));
//
//                for (Object o : isoCoords) {
//
//                    Row r = (Row) o;
//
//                    int unp_start = r.getInt(r.fieldIndex(CommonConstants.COL_START));
//                    int unp_end = r.getInt(r.fieldIndex(CommonConstants.COL_END));
//                    int[] pdb = converter.convert1to2(unp_start, unp_end);
//
//                    if (pdb[0] == -1 && pdb[1] != -1)
//                        unp_start = converter.s1;
//                    if (pdb[1] == -1 && pdb[0] != -1)
//                        unp_end = converter.e1;
//
//                    iso.getIsoformCoordinates().add(new CoordinatesRange(unp_start, unp_end));
//                    iso.getStructureCoordinates().add(new CoordinatesRange(pdb[0], pdb[1]));
//
//                }
//            }
//
//        }



        return null;
    }
}
