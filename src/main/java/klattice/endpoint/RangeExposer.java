package klattice.endpoint;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class RangeExposer {
    private static final Pattern RANGE_PAT = Pattern.compile(".*?bytes=(\\d+)-(\\d+)$");
    private final HttpHeaders httpHeaders;

    public RangeExposer(HttpHeaders httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    public Optional<Response> answerWithFile(File file) throws IOException {
        var raf = new RandomAccessFile(file, "r");
        var rangeStr = httpHeaders.getHeaderString("Range");
        if (Objects.isNull(rangeStr)) {
            return Optional.empty();
        }
        var matcher = RANGE_PAT.matcher(rangeStr);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        var start = Long.parseLong(matcher.group(1));
        var end = Long.parseLong(matcher.group(2));
        var len = (end - start) + 1;
        raf.seek(start);
        var bytesStr = String.format("bytes %s-%s/%d", start, end, raf.length());
        var response = Response.status(206).entity((StreamingOutput) output -> {
                    long byteCount = 0;
                    int byteV;
                    while ((byteCount < len) && (byteV = raf.read()) != -1) {
                        output.write(byteV);
                        byteCount++;
                    }
                    raf.close();
                })
                .header("Accept-Ranges", "bytes")
                .header("Content-Range", bytesStr)
                .header("Content-Length", String.format("%d", len))
                .build();
        return Optional.of(response);
    }
}
