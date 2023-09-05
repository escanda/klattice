package klattice.wire.msg.server;

import klattice.wire.msg.*;

import java.util.List;

public record AuthenticationOk() implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.AuthenticationOk.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(new MessageField<>("success", signedInt())));
    }

    @Override
    public Object[] values() {
        return new Object[] { 0 };
    }
}
