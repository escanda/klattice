package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Resource;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import klattice.file.CsvToAvroParquetExporter;
import org.jboss.logging.Logger;

import java.io.*;

@Resource
@Path("/sys-table/{tableName}.parquet")
public class SysTableExportResource {
    @LoggerName("SysInfoExportResource")
    Logger logger;

    @Inject
    CsvToAvroParquetExporter csvToAvroParquetExporter;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Blocking
    public Response withName(@PathParam("tableName") String tableName, @Context HttpHeaders httpHeaders) throws IOException {
        var tempFile = File.createTempFile(tableName, "parquet");
        var is = getClass().getResourceAsStream("/tables/" + tableName + ".csv");
        var os = new FileOutputStream(tempFile);
        try (is; os) {
            csvToAvroParquetExporter.export(tableName, is, os);
            return new RangeExposer(httpHeaders).answerWithFile(tempFile).orElseGet(() -> {
                try {
                    return Response.status(200).entity(new FileInputStream(tempFile)).build();
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @HEAD
    @Blocking
    public Response head(@PathParam("tableName") String tableName) throws IOException {
        logger.warn("Head over " + tableName);
        return Response.accepted().header("Accept-Ranges", "bytes").build();
    }
}
