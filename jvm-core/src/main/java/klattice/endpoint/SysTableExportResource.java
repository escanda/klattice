package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Resource;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.*;
import klattice.registry.SchemaRegistryResource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;

@Resource
@Path("/sys-table")
public class SysTableExportResource {
    @LoggerName("SysInfoExportResource")
    Logger logger;

    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    /**
     * Returns a CSV view of the available schemas in SchemaRegistry.
     * @param httpHeaders
     * @return
     * @throws IOException
     */
    @Path("/schemas")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Blocking
    public Response schemas(@Context HttpHeaders httpHeaders) throws IOException {
        StreamingOutput stream = os -> {
            try (CSVPrinter writer = new CSVPrinter(new OutputStreamWriter(os), CSVFormat.DEFAULT)) {

            } catch (IOException ex) {
                logger.errorv("Cannot write CSV to output streaming in streaming response", ex);
            }
        };
        return Response.ok().entity(stream).build();
    }
}
