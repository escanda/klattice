package klattice.calcite;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

    public static Object schemaRegistryForTopic(String schemaRegistryUrl, String topicName) {
        SchemaRegi
        return null;
    }
}
