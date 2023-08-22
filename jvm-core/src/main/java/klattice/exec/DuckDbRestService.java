package klattice.exec;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "duckdb-service-api")
public interface DuckDbRestService {
    @POST
    @Path("/upsert-session")
    Response makeSession(int id);

    @POST
    @Path("/exec-arbitrary")
    void execArbitrarySql(@QueryParam("session_id") int sessionId, String body);

    @POST
    @Path("/exec-substrait")
    Response execSubstrait(@QueryParam("session_id") int sessionId, byte[] payload);

    @POST
    @Path("/make-session")
    @Consumes({MediaType.TEXT_PLAIN})
    int makeSession();
}
