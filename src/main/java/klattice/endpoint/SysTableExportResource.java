package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Resource;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.*;
import klattice.schema.SchemaRegistryResource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

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

    /**
     * Returns a CSV view of the available schemas in SchemaRegistry.
     * @param httpHeaders
     * @return
     * @throws IOException
     */
    @Path("/{tableName}.parquet")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Blocking
    public Response withName(@PathParam("tableName") String tableName, @Context HttpHeaders httpHeaders) throws IOException {
        StreamingOutput stream = os -> {
            var csvFormat = CSVFormat.DEFAULT;
            try (CSVPrinter writer = new CSVPrinter(new OutputStreamWriter(os), csvFormat)) {
                writer.printRecord((Object[]) csvFormat.builder().setHeader("value", "kind").build().getHeader());
                writer.printRecord(List.of("HEHE-" + tableName));
            } catch (IOException ex) {
                logger.errorv("Cannot write CSV to output streaming in streaming response", ex);
            }
        };
        return Response.ok().entity(stream).build();
    }
}
