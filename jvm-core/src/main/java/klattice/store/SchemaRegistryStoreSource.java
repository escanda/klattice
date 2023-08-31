package klattice.store;

import jakarta.enterprise.context.Dependent;
import klattice.registry.SchemaRegistryResource;
import klattice.schema.SchemaMetadata;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import java.util.stream.Stream;

@Dependent
public class SchemaRegistryStoreSource implements DatabaseStoreSource {
    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    @Override
    public Stream<SchemaMetadata> allSchemas() {
        return schemaRegistryResource.allSubjects()
                .stream()
                .map(schemaSubject -> new SchemaMetadata(schemaSubject.name()));
    }

    @Override
    public Stream<SchemaMetadata> byPrefix(String prefix) {
        return schemaRegistryResource.allSubjectsByPrefix(prefix)
                .stream()
                .map(schemaSubject -> new SchemaMetadata(schemaSubject.name()));
    }
}
