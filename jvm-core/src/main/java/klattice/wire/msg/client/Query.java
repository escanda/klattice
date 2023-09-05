package klattice.wire.msg.client;

import klattice.wire.msg.*;
import klattice.wire.msg.type.StrV;

import java.util.List;

public record Query(String query) implements Message {
    @Override
    public char command() {
        return PgsqlClientCommandType.Query.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("query", StrV::new)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { query };
    }
}
