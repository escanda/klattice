package klattice.wire.msg.client;

import klattice.wire.msg.Message;
import klattice.wire.msg.PgsqlClientCommandType;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;

import java.util.List;

public record Sync() implements Message {
    @Override
    public char command() {
        return PgsqlClientCommandType.Sync.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of());
    }

    @Override
    public Object[] values() {
        return new Object[0];
    }
}
