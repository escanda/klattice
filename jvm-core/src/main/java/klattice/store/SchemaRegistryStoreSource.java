package klattice.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.registry.SchemaEntity;
import klattice.registry.SchemaRegistryService;
import klattice.row.RowConverter;
import klattice.row.RowTypeColumnInfo;
import klattice.row.RowTypeInfo;
import klattice.row.RowValueType;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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

    private RowTypeInfo parseSchema(SchemaEntity.SchemaType schemaType, String schema) throws JsonProcessingException {
        var jsonNode = om.readTree(schema); // TODO: assume `schemaType' is AVRO
        var fieldsNode = jsonNode.get("fields");
        var cols = new ArrayList<RowTypeColumnInfo>();
        for (JsonNode fieldNode : fieldsNode) {
            var name = fieldNode.get("name").asText();
            var typeName = fieldNode.get("type").asText();
            cols.add(new RowTypeColumnInfo(name, RowValueType.valueOf(typeName), Optional.of((RowConverter) Function.identity())));
        }
        return new RowTypeInfo(cols);
    }
}
