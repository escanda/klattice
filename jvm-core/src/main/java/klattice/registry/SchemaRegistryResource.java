package klattice.registry;

import jakarta.ws.rs.*;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.util.List;

@RegisterRestClient(configKey = "schema-registry-api")
public interface SchemaRegistryResource {
    String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    @GET
    @Path("/schemas/types")
    @Produces({ CONTENT_TYPE })
    List<String> types();

    @GET
    @Path("/schemas/ids/{id}")
    @Consumes({ CONTENT_TYPE })
    SchemaSubject byId(@PathParam("id") int id);

    @GET
    @Path("/subjects/{subject}/versions/-1")
    @Consumes({ CONTENT_TYPE })
    SchemaSubject byTopicName(@PathParam("subject") String subject);

    @POST
    @Path("/subjects/{subject}/versions")
    @Produces({ CONTENT_TYPE })
    @Consumes({ CONTENT_TYPE })
    SchemaSubject add(@PathParam("subject") String subject, SchemaEntry schemaEntry);

    @GET
    @Path("/subjects")
    @Consumes({ CONTENT_TYPE })
    List<String> allSubjects();

    @GET
    @Path("/subjects")
    @Consumes({ CONTENT_TYPE })
    List<String> allSubjectsByPrefix(@QueryParam("subjectPrefix") String prefix);
}
