package klattice.store;

public interface DatabaseStoreSource {
    Iterable<SchemaMetadata> allSchemas();
}
