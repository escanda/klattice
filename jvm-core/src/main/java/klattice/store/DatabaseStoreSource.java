package klattice.store;

import klattice.schema.SchemaMetadata;

import java.util.stream.Stream;

public interface DatabaseStoreSource {
    Stream<SchemaMetadata> allSchemas();
    Stream<SchemaMetadata> byPrefix(String prefix);
}
