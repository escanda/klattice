package klattice.store;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import klattice.schema.SchemaMetadata;

import java.util.Collection;
import java.util.stream.Stream;

@ApplicationScoped
public class TransientStore {
    @Inject
    Instance<DatabaseStoreSource> sources;

    public Collection<SchemaMetadata> metadata() {
        return sources.stream().flatMap(this::fetchMetadata).toList();
    }

    protected Stream<SchemaMetadata> fetchMetadata(DatabaseStoreSource source) {
        return source.allSchemas();
    }
}
