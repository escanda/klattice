package klattice.wire;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public class PgSession implements Closeable {
    private static final String SESSION_ATTR = "session";
    public static Optional<PgSession> of(Channel channel) {
        var attributeKey = AttributeKey.valueOf(SESSION_ATTR);
        if (!channel.hasAttr(attributeKey)) {
            var session = new PgSession(channel);
            channel.attr(attributeKey).setIfAbsent(session);
        }
        return Optional.ofNullable((PgSession) channel.attr(attributeKey).get());
    }

    private final Channel channel;

    private PgSession(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }
}
