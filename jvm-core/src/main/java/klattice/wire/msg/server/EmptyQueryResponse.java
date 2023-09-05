package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.PgsqlServerCommandType;

import java.util.List;

public record EmptyQueryResponse() implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.EmptyQueryResponse.id;
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
