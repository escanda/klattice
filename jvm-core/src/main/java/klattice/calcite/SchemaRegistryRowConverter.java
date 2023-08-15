package klattice.calcite;

import com.fasterxml.jackson.databind.type.TypeFactory;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import klattice.msg.Environment;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.apache.calcite.adapter.kafka.KafkaRowConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class SchemaRegistryRowConverter implements KafkaRowConverter<byte[], byte[]> {
    private static final Logger LOGGER = Logger.getLogger(SchemaRegistryRowConverter.class);
    private final TypeFactory typeFactory;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<Integer, ParsedSchema> parsedSchemaMap = new LinkedHashMap<>();
    private final Map<String, Integer> relSchemaMap = new LinkedHashMap<>();

    public SchemaRegistryRowConverter(TypeFactory typeFactory, SchemaRegistryClient schemaRegistryClient, Environment environ) throws IOException, RestClientException {
        this.typeFactory = typeFactory;
        this.schemaRegistryClient = schemaRegistryClient;
        for (Schema schema : environ.getSchemasList()) {
            for (Rel rel : schema.getRelsList()) {
                var parsedSchema = schemaRegistryClient.getSchemaById(rel.getSchemaId());
                parsedSchemaMap.put(rel.getSchemaId(), parsedSchema);
                relSchemaMap.put(rel.getRelName(), rel.getSchemaId());
            }
        }
    }

    @Override
    public RelDataType rowDataType(String topicName) {
        if (relSchemaMap.containsKey(topicName)) {
            var mappedId = relSchemaMap.get(topicName);
            var parsedSchema = parsedSchemaMap.get(mappedId);
            return null;
        } else {
            return null;
        }
    }

    @Override
    public Object[] toRow(ConsumerRecord<byte[], byte[]> message) {
        return new Object[0];
    }
}
