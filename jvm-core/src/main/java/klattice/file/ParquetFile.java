package klattice.file;

import klattice.msg.Batch;

import java.io.*;

public class ParquetFile implements AutoCloseable {
    public static ParquetFile of(File file) throws IOException {
        return new ParquetFile(file);
    }

    private final OutputStreamWriter osw;

    public ParquetFile(File file) throws FileNotFoundException {
        this.osw = new OutputStreamWriter(new FileOutputStream(file, true));
    }

    @Override
    public void close() throws Exception {
        this.osw.close();
    }

    public long append(Batch batch) throws IOException {
        for (int i = 0; i < batch.getRowsCount(); i++) {
            var row = batch.getRowsList().get(i);
            row.writeTo(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    osw.write(b);
                }
            });
        }
        return 0;
    }
}
