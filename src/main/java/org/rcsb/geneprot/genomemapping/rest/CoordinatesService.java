package org.rcsb.geneprot.genomemapping.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

/**
 * Created by Yana Valasatava on 11/15/17.
 */

@Path("/coordinates")
public class CoordinatesService {

    @GET
    @Path("/{id}")
    public Response getMsg(@PathParam("id") String msg) {

        String output = "Jersey say : " + msg;

        return Response.status(200).entity(output).build();

    }
}
