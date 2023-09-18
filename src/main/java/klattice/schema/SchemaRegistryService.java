package klattice.schema;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import java.util.List;

@ApplicationScoped
public class SchemaRegistryService {
    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    public List<SchemaEntity> schemaEntities() {
        return schemaRegistryResource.allSubjects().stream()
                .map(topicName -> schemaRegistryResource.byTopicName(topicName))
                .map(schemaSubject -> new SchemaEntity(
                        schemaSubject.subject(),
                        schemaSubject.id(),
                        schemaSubject.schema(),
                        schemaSubject.schemaType() == null ?
                                null :
                                SchemaEntity.SchemaType.valueOf(schemaSubject.schemaType()))
                )
                .toList();
    }
}
