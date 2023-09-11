package klattice.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.log.LoggerName;
import io.substrait.type.ImmutableType;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.registry.SchemaEntity;
import klattice.registry.SchemaRegistryService;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Objects;

@Dependent
public class SchemaRegistryStoreSource implements DatabaseStoreSource {
    @LoggerName("SchemaRegistryStoreSource")
    Logger logger;

    @Inject
    SchemaRegistryService schemaRegistryService;

    @Inject
    ObjectMapper om;

    @Override
    public Iterable<SchemaMetadata> allSchemas() {
        return () -> schemaRegistryService.schemaEntities().stream().map(schemaEntity -> {
                    try {
                        return new SchemaMetadata(
                                schemaEntity.id(),
                                schemaEntity.name(),
                                parseSchema(schemaEntity.schemaType(), schemaEntity.schema())
                        );
                    } catch (JsonProcessingException e) {
                        logger.errorv("Cannot process schema JSON", e);
                        return null;
                    }
                }).filter(Objects::isNull).map(SchemaMetadata.class::cast)
                .iterator();
    }

    private TypeAndName parseSchema(SchemaEntity.SchemaType schemaType, String schema) throws JsonProcessingException {
        var jsonNode = om.readTree(schema); // TODO: assume `schemaType' is AVRO
        var fieldsNode = jsonNode.get("fields");
        var cols = new ArrayList<TypeAndName.TypeKind>();
        var rootName = jsonNode.get("name").asText();
        for (JsonNode fieldNode : fieldsNode) {
            var name = fieldNode.get("name").asText();
            // TODO: var typeName = fieldNode.get("type").asText();
            var builder = ImmutableType.VarChar.builder();
            builder.nullable(true);
            builder.length(-1);
            var typeTerminal = new TypeAndName.TypeTerminal(name, builder.build());
            cols.add(typeTerminal);
        }
        return new TypeAndName(new TypeAndName.TypeLeaf(cols), rootName);
    }
}
