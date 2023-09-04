package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;

import java.util.List;

public record AuthenticationOk() implements Message {
    @Override
    public char command() {
        return 'R';
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
