package klattice.file;

import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.msg.Batch;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class Files {
    private final File baseRoot;
    @LoggerName("Files")
    Logger logger;

    final Map<String, byte[]> byteMap = new LinkedHashMap<>();

    @Inject
    public Files(@ConfigProperty(name = "klattice.baseroot") File baseRoot) {
        this.baseRoot = baseRoot;
    }

    public void put(String key, byte[] value) {
        byteMap.put(key, value);
    }

    public Optional<byte[]> get(String key) {
        if (byteMap.containsKey(key)) {
            var bytes = byteMap.get(key);
            return Optional.of(bytes);
        } else {
            return Optional.empty();
        }
    }

    public long append(Stream<Batch> batchStream, Schema schema, Rel rel) throws IOException {
        var file = new File(baseRoot, schema.getSchemaId() + "." + rel.getRelName());
        var tableFile = ParquetFile.of(file);
        try {
            var rowCount = batchStream.mapToLong(batch -> {
                try {
                    return tableFile.append(batch);
                } catch (IOException e) {
                    logger.errorv("Cannot append batch to table file by id %d", schema.getSchemaId());
                    return 0L;
                }
            }).sum();
            return rowCount;
        } finally {
            try {
                tableFile.close();
            } catch (Exception e) {
                throw new IOException("Cannot close stream", e);
            }
        }
    }
}
