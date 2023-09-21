package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.file.AvroParquetExport;
import klattice.schema.SchemaSubject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ApplicationScoped
public class KafkaExporter {
    private static final String BYTEARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private static final String RESET_VALUE = "earliest";

    @LoggerName("Export")
    Logger logger;

    @Inject
    CommandLine.ParseResult parseResult;

    public void export(@Nonnull SchemaSubject schemaSubject, @Nonnull String topicName, @Nonnull OutputStream outputStream, long rowLimit) throws IOException {
        var props = new Properties();
        var bootstrapServers = parseResult.matchedOptionValue('b', "127.0.0.1:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BYTEARRAY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BYTEARRAY_DESERIALIZER);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, RESET_VALUE);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaExporter.class.getSimpleName());

        try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            var schemaStr = schemaSubject.schema();
            var parser = new Schema.Parser();
            var schema = parser.parse(schemaStr);
            try (var export = new AvroParquetExport(outputStream, schema)) {
                var assignment = consumer.assignment();
                consumer.seekToBeginning(assignment);
                long rowCount = 0;
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                loop:
                {
                    while (true) {
                        var poll = consumer.poll(Duration.ofMillis(1500));
                        if ((poll.isEmpty() && rowLimit == 0) || rowCount >= rowLimit) {
                            logger.trace("No more records");
                            break loop;
                        } else {
                            for (var consumerRecord : poll) {
                                var decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), null);
                                var reader = new GenericDatumReader<GenericData.Record>(schema);
                                var record = reader.read(null, decoder);
                                export.record(record);
                                rowCount++;
                                var key = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                                var offset = new OffsetAndMetadata(consumerRecord.offset());
                                offsets.put(key, offset);
                                if (rowLimit > 0 && rowCount >= rowLimit) {
                                    break loop;
                                }
                            }
                            export.flush();
                        }
                    }
                }
                consumer.commitSync(offsets);
                export.flush();
            }
        }
    }
}
