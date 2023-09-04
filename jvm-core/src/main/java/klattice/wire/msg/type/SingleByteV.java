package klattice.wire.msg.type;

import io.netty.buffer.ByteBuf;
import klattice.wire.msg.PgsqlValueType;

public class SingleByteV extends PgsqlValueType<Byte> {
    public static final PgsqlValueType<Byte> I = new SingleByteV();
    @Override
    public int byteSize(Object value) {
        return 1;
    }

    @Override
    public void serializeInto(Object value, ByteBuf buf) {
        buf.writeByte(cast(value));
    }

    @Override
    public Byte cast(Object value) {
        if (value instanceof Character) {
            return (byte) ((Character) value).charValue();
        } else {
            return (Byte) value;
        }
    }
}
