package klattice.exec;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "schema-registry-api")
public interface SchemaRegistryResource {
    String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    @GET
    @Path("/schemas/types")
    @Produces({ CONTENT_TYPE })
    Response types();

    @GET
    @Path("/schemas/ids/{id}")
    @Consumes({ CONTENT_TYPE })
    Response byId(int id);

    @GET
    @Path("/subjects/{subject}/versions/-1")
    @Consumes({ CONTENT_TYPE })
    Response byTopicName(@PathParam("subject") String subject);

    @POST
    @Path("/subjects/{subject}/versions")
    @Produces({ CONTENT_TYPE })
    @Consumes({ CONTENT_TYPE })
    Response add(@PathParam("subject") String subject, JsonNode node);
}
