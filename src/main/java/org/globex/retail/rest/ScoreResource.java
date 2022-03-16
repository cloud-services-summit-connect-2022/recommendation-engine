package org.globex.retail.rest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.smallrye.mutiny.Uni;
import org.globex.retail.streams.AggregatedScoreQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Path("/")
public class ScoreResource {

    private static final Logger log = LoggerFactory.getLogger(ScoreResource.class);

    @Inject
    AggregatedScoreQuery aggregatedScoreQuery;

    @GET
    @Path("/score/{category}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getScore(@PathParam("category") String category) {
        return aggregatedScoreQuery.getAggregatedScore(category)
                .onItem().transform(s -> {
                    if (s == null || s.isEmpty()) {
                        return Response.status(Response.Status.NOT_FOUND.getStatusCode()).build();
                    } else {
                        return Response.ok(s).build();
                    }
                }).onFailure().recoverWithItem(throwable -> {
                    log.error("Exception when processing payload", throwable);
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Processing error").build();
                });
    }

}
