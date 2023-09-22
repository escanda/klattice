package klattice.file;

import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.Objects.requireNonNull;

@ApplicationScoped
public class CsvToAvroParquetExporter {
    @LoggerName("CsvToAvroParquetExporter")
    Logger logger;

    public void export(String name, InputStream csvInputStream, OutputStream parquetOutputStream) throws IOException {
        try (var parser = CSVParser.parse(csvInputStream, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            long i = 0L;
            Schema schema = null;
            AvroParquetExport avroParquetExport = null;
            for (CSVRecord record : parser) {
                if (i < 1) {
                    var headerValues = Arrays.stream(record.values())
                            .map(CsvToAvroParquetExporter::parseHeaderValue)
                            .toList();
                    schema = toSchema(name, headerValues);
                    avroParquetExport = new AvroParquetExport(parquetOutputStream, schema);
                } else {
                    assert avroParquetExport != null;
                    var r = new GenericData.Record(schema);
                    int j = 0;
                    for (String value : record.values()) {
                        r.put(j, value);
                        ++j;
                    }
                    avroParquetExport.record(r);
                }
                i++;
            }
            if (avroParquetExport != null) {
                avroParquetExport.flush();
                avroParquetExport.close();
            }
        }
    }

    private Schema toSchema(String name, List<NameAndType> nameAndTypes) {
        var schema = Schema.createRecord(name, null, null, false);
        schema.setFields(nameAndTypes.stream().map(nameAndType -> new Schema.Field(nameAndType.name(), Schema.create(requireNonNull(toType(nameAndType.type()))))).toList());
        return schema;
    }

    private Schema.Type toType(String type) {
        switch (type) {
            case "VARCHAR" -> {
                return Schema.Type.STRING;
            }
        }
        logger.warnv("Cannot translate type {0} so coerced to NULL", type);
        return Schema.Type.NULL;
    }

    private static NameAndType parseHeaderValue(String value) {
        var scanner = new Scanner(value).useDelimiter("[()]");
        String name;
        String type = null;
        Collection<String> typeParameters = new ArrayList<>();
        if (scanner.hasNext()) {
            name = scanner.next();
            type = scanner.next();
        } else {
            name = value;
        }
        return new NameAndType(name, type, typeParameters);
    }

    private record NameAndType(String name, String type, Collection<String> typeParameters) {}
}
