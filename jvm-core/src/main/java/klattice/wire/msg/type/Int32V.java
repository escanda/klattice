package klattice.wire.msg.type;

import io.netty.buffer.ByteBuf;
import klattice.wire.msg.PgsqlValueType;

public class Int32V extends PgsqlValueType<Integer> {
    @Override
    public int byteSize(Object value) {
        return 4;
    }

    @Override
    public void serializeInto(Object value, ByteBuf buf) {
        buf.writeInt(cast(value));
    }

    @Override
    public Integer cast(Object value) {
        return ((Number) value).intValue();
    }
}
