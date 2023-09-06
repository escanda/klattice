package klattice.wire.msg.server;

import klattice.wire.msg.*;
import klattice.wire.msg.type.Int32V;

import java.util.List;

public record BackendKeyData(int processId, int secretKey) implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.BackendKeyData.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("processId", Int32V::new),
                new MessageField<>("secretKey", Int32V::new)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { processId(), secretKey() };
    }
}
