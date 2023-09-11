package klattice.endpoint;

import io.quarkus.arc.log.LoggerName;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.registry.SchemaSubject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ApplicationScoped
public class Exporter {
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Exporter.class.getSimpleName());

        try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            var schema = schemaSubject.schema();
            var parser = new Schema.Parser();
            var parsedSchema = parser.parse(schema);
            var bos = new BufferedOutputStream(outputStream);
            var pbw = new ParquetBufferedWriter(bos);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                    <GenericData.Record>builder(pbw)
                    .withSchema(parsedSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
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
                                var reader = new GenericDatumReader<GenericData.Record>(parsedSchema);
                                var record = reader.read(null, decoder);
                                writer.write(record);
                                rowCount++;
                                var key = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                                var offset = new OffsetAndMetadata(consumerRecord.offset());
                                offsets.put(key, offset);
                                if (rowLimit > 0 && rowCount >= rowLimit) {
                                    break loop;
                                }
                            }
                            bos.flush();
                        }
                    }
                }
                consumer.commitSync(offsets);
                bos.flush();
            }
        }
    }
}
