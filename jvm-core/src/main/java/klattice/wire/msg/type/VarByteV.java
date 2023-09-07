package klattice.wire.msg.type;

import io.netty.buffer.ByteBuf;
import klattice.wire.msg.PgsqlValueType;

public class VarByteV extends PgsqlValueType<byte[]> {
    @Override
    public int byteSize(Object value) {
        return cast(value).length;
    }

    @Override
    public void serializeInto(Object value, ByteBuf buf) {
        for (byte b : cast(value)) {
            buf.writeByte(b);
        }
    }

    @Override
    public byte[] cast(Object value) {
        return (byte[]) value;
    }
}
