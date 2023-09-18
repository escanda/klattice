package klattice.wire.msg.client;

import klattice.wire.msg.Message;
import klattice.wire.msg.PgsqlClientCommandType;
import klattice.wire.msg.PgsqlPayloadProvider;

public record Terminate() implements Message {
    @Override
    public char command() {
        return PgsqlClientCommandType.Terminate.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return emptyProvider();
    }

    @Override
    public Object[] values() {
        return new Object[0];
    }
}
