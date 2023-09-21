package klattice.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class AvroParquetExport implements Closeable {
    private final OutputStream os;
    private final ParquetWriter<GenericData.Record> writer;

    public AvroParquetExport(OutputStream os, Schema schema) throws IOException {
        this.os = os;
        var bos = new BufferedOutputStream(os);
        var pbw = new ParquetBufferedWriter(bos);
        writer = AvroParquetWriter.
                <GenericData.Record>builder(pbw)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    public void record(GenericData.Record record) throws IOException {
        writer.write(record);
    }

    public void flush() throws IOException {
        this.os.flush();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
