package klattice.file;

import jakarta.enterprise.context.Dependent;
import klattice.msg.Batch;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

@Dependent
public class Files {
    @ConfigProperty(name = "klattice.baseroot")
    File baseRoot;

    public long append(Stream<Batch> batchStream, Schema schema, Rel rel) throws IOException {
        var file = new File(baseRoot, schema.getSchemaId() + "." + rel.getRelName());
        var tableFile = ParquetFile.of(file);
        var rowCount = batchStream.mapToLong(tableFile::append).sum();
        return rowCount;
    }
}
