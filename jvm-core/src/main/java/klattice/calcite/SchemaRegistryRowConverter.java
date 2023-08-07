package klattice.calcite;

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
import java.util.List;

public class SchemaRegistryRowConverter implements KafkaRowConverter {
    private static final Logger LOGGER = Logger.getLogger(SchemaRegistryRowConverter.class);
    private final SchemaRegistryClient schemaRegistryClient;

    public SchemaRegistryRowConverter(SchemaRegistryClient schemaRegistryClient, Environment environ) throws IOException, RestClientException {
        this.schemaRegistryClient = schemaRegistryClient;
        for (Schema schema : environ.getSchemasList()) {
            for (Rel rel : schema.getRelsList()) {
                var parsedSchema = schemaRegistryClient.getSchemaById(rel.getSchemaId());
            }
        }
    }

    @Override
    public RelDataType rowDataType(String topicName) {
        try {
            List<ParsedSchema> schemas = schemaRegistryClient.getSchemas(topicName, false, true);
        } catch (IOException | RestClientException e) {
            LOGGER.errorv(e, "Cannot get schemas for topic name '{0}'", topicName);
        }
        return null;
    }

    @Override
    public Object[] toRow(ConsumerRecord message) {
        return new Object[0];
    }
}
