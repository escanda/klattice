package klattice.wire.msg.type;

import io.netty.buffer.ByteBuf;
import klattice.wire.msg.PgsqlValueType;

import java.nio.charset.StandardCharsets;

public class StrV extends PgsqlValueType<String> {
    @Override
    public int byteSize(Object value) {
        return cast(value).getBytes(StandardCharsets.UTF_8).length + 1;
    }

    @Override
    public void serializeInto(Object value, ByteBuf buf) {
        buf.writeCharSequence(cast(value), StandardCharsets.UTF_8);
        buf.writeByte(0);
    }

    @Override
    public String cast(Object value) {
        return value.toString();
    }
}
