package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public interface Util {
    static Optional<String> readCstring(ByteBuf buf) {
        int len = buf.bytesBefore((byte) 0);
        String string = buf.readBytes(len).toString(StandardCharsets.UTF_8);
        return string.isEmpty() ? Optional.empty() : Optional.of(string);
    }
}
