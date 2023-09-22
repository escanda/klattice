package klattice.wire.msg;

import io.netty.buffer.ByteBuf;

public abstract class PgsqlValueType<T> {
    protected PgsqlValueType() {}

    public abstract int byteSize(Object value);

    public abstract void serializeInto(Object value, ByteBuf buf);

    public abstract T cast(Object value);
}
