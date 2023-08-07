package klattice.file;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@QuarkusTest
public class FilesTest {
    @TestHTTPEndpoint(TopicFileResource.class)
    @TestHTTPResource("topic_a")
    URL url;

    @Inject
    Files files;

    @Test
    public void testTopicA() throws IOException {
        files.put("topic_a", "abcde".getBytes(StandardCharsets.UTF_8));
        try (InputStream in = url.openStream()) {
            String contents = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Assertions.assertEquals("abcde", contents);
        }
    }
}
