package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Resource;
import jakarta.ws.rs.*;
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
@Path("/sys-table/{tableName}.parquet")
public class SysTableExportResource {
    @LoggerName("SysInfoExportResource")
    Logger logger;

    @RestClient
    SchemaRegistryResource schemaRegistryResource;

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

    @HEAD
    @Blocking
    public Response head(@PathParam("tableName") String tableName, @QueryParam("limit") int rowLimit) throws IOException {
        logger.warn("Head over " + tableName);
        return Response.accepted().header("Accept-Ranges", "bytes").build();
    }
}
