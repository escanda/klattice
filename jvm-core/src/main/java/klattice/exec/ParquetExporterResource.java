package klattice.exec;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.arc.log.LoggerName;
import io.smallrye.common.annotation.Blocking;
import jakarta.annotation.Resource;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

@Resource
@Path("/{topicName}")
public class ParquetExporterResource {
    private final static Pattern RANGE_PAT = Pattern.compile(".*?bytes=(\\d+)-(\\d+)$");
    @LoggerName("ParquetExporterResource")
    Logger logger;

    @Inject
    Exporter exporter;

    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    private final Map<String, File> topicFileMap = new HashMap<>();

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Blocking
    public Response get(@PathParam("topicName") String path, @Context HttpHeaders httpHeaders, @QueryParam("limit") int rowLimit) throws IOException {
        var topicName = path.replaceAll("\\.parquet$", "");
        File file;
        if (!topicFileMap.containsKey(topicName)) {
            var dump = getExistingDump(rowLimit, topicName);
            if (dump.isEmpty()) throw new NotFoundException();
            file = dump.get();
        } else {
            file = topicFileMap.get(topicName);
        }
        var r = new RandomAccessFile(file, "r");
        var rangeStr = httpHeaders.getHeaderString("Range");
        if (!Objects.isNull(rangeStr)) {
            var matcher = RANGE_PAT.matcher(rangeStr);
            if (matcher.matches()) {
                var start = Integer.parseInt(matcher.group(1));
                var end = Integer.parseInt(matcher.group(2));
                var len = (end - start) + 1;
                r.seek(start);
                var bytesStr = String.format("bytes %s-%s/%d", start, end, r.length());
                return Response.status(206).entity((StreamingOutput) output -> {
                            int byteCount = 0;
                            int byteV;
                            while ((byteCount < len) && (byteV = r.read()) != -1) {
                                output.write(byteV);
                                byteCount++;
                            }
                            r.close();
                        })
                        .header("Accept-Ranges", "bytes")
                        .header("Content-Range", bytesStr)
                        .header("Content-Length", String.format("%d", len)).build();
            }
            throw new NotFoundException("No HEAD prior request for topic " + topicName);
        } else {
            return Response.status(200).entity(new FileInputStream(file)).build();
        }
    }

    @HEAD
    @Blocking
    public Response head(@PathParam("topicName") String path, @QueryParam("limit") int rowLimit) throws IOException {
        var topicName = path.replaceAll("\\.parquet$", "");
        getExistingDump(rowLimit, topicName).ifPresent(file -> logger.infov("Created file {0}", file));
        return Response.accepted().header("Accept-Ranges", "bytes").build();
    }

    private Optional<File> getExistingDump(int rowLimit, String topicName) throws IOException {
        var file = tmpFileForTopicName(topicName);
        topicFileMap.put(topicName, file);
        try (var fos = new FileOutputStream(file, true)) {
            logger.infov("Starting topic export by name '{0}'", new Object[]{topicName});
            final JsonNode jsonNode;
            try (var schemaResponse = schemaRegistryResource.byTopicName(topicName)) {
                jsonNode = schemaResponse.readEntity(JsonNode.class);
            } catch (WebApplicationException e) {
                logger.error("Cannot find topic by subject name", e);
                return Optional.empty();
            }
            exporter.export(jsonNode, topicName, fos, rowLimit);
        }
        return Optional.of(file);
    }

    private File tmpFileForTopicName(String topicName) throws IOException {
        return Files.createTempFile(ParquetExporterResource.class.getSimpleName(), topicName + ".parquet").toFile();
    }
}
