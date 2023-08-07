package klattice.file;

import klattice.msg.Batch;

import java.io.*;

public class ParquetFile implements AutoCloseable {
    private final File file;
    private final OutputStreamWriter os;

    public ParquetFile(File file) throws FileNotFoundException {
        this.file = file;
        this.os = new OutputStreamWriter(new FileOutputStream(file, true));
    }

    public File getFile() {
        return file;
    }

    @Override
    public void close() throws Exception {
    }

    public static ParquetFile of(File file) throws IOException {
        return new ParquetFile(file);
    }

    public long append(Batch batch) {
        return 0;
    }
}
