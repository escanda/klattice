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
import klattice.file.KafkaExporter;
import klattice.schema.SchemaRegistryResource;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Resource
@Path("/topic-table/{topicName}.parquet")
public class ParquetExportResource {
    @LoggerName("ParquetExporterResource")
    Logger logger;

    @Inject
    KafkaExporter kafkaExporter;

    @Inject
    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    private final Map<String, File> topicFileMap = new HashMap<>();

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
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
        return new RangeExposer(httpHeaders).answerWithFile(file).orElseGet(() -> {
            try {
                return Response.status(200).entity(new FileInputStream(file)).build();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
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
            var schemaSubject = schemaRegistryResource.byTopicName(topicName);
            kafkaExporter.export(schemaSubject, topicName, fos, rowLimit);
        }
        return Optional.of(file);
    }

    private File tmpFileForTopicName(String topicName) throws IOException {
        return Files.createTempFile(ParquetExportResource.class.getSimpleName(), topicName + ".parquet").toFile();
    }
}
