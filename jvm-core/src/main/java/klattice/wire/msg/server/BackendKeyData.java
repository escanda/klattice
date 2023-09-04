package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.type.Int32V;

import java.util.List;

public record BackendKeyData(int processId, int secretKey) implements Message {
    @Override
    public char command() {
        return 'K';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("processId", () -> Int32V.I),
                new MessageField<>("secretKey", () -> Int32V.I)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { processId(), secretKey() };
    }
}
