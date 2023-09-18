package klattice.wire.msg.type;

import io.netty.buffer.ByteBuf;
import klattice.wire.msg.PgsqlValueType;

public class Int16V extends PgsqlValueType<Short> {
    @Override
    public int byteSize(Object value) {
        return 2;
    }

    @Override
    public void serializeInto(Object value, ByteBuf buf) {
        buf.writeShort(cast(value));
    }

    @Override
    public Short cast(Object value) {
        return ((Number) value).shortValue();
    }
}
