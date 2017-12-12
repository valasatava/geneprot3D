package org.rcsb.geneprot.genomemapping.sandbox;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Created by Yana Valasatava on 12/1/17.
 */
public class test {

    public static void main(String[] args) throws UnirestException {

        String uniProtId = "O33897";

        HttpResponse<JsonNode> response = Unirest
                .get("https://www.ebi.ac.uk/proteins/api/proteins/{id}/isoforms.json")
                .routeParam("id", uniProtId)
                .asJson();

        if (response.getStatus() == 404)
            response = Unirest
                    .get("https://www.ebi.ac.uk/proteins/api/proteins/{id}.json")
                    .routeParam("id", uniProtId)
                    .asJson();

        if (response.getStatus() != 200)
            System.out.println(response.getStatusText() + " " + response.getStatus());
    }
}
