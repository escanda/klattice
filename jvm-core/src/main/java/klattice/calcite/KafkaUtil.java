package klattice.calcite;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

class KafkaUtil {
    public static Long countTopic(String bootstrap, String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            Set<TopicPartition> assignment;
            while ((assignment = consumer.assignment()).isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
            assert (endOffsets.size() == beginningOffsets.size());
            assert (endOffsets.keySet().equals(beginningOffsets.keySet()));

            Long totalCount = beginningOffsets.entrySet().stream().mapToLong(entry -> {
                TopicPartition tp = entry.getKey();
                Long beginningOffset = entry.getValue();
                Long endOffset = endOffsets.get(tp);
                return endOffset - beginningOffset;
            }).sum();
            return totalCount;
        }
    }

    public static Optional<Relation> schemaRegistryForTopic(String schemaRegistryUrl, String topicName) {
        var schemaRegistryClient = SchemaRegistryClientFactory.newClient(
                List.of(schemaRegistryUrl),
                1024,
                List.of(new AvroSchemaProvider()),
                Map.of(),
                Map.of()
        );
        return Optional.empty();
    }

    public record Relation(List<RelDataType> types, List<String> fields) {}
}
