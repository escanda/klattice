package klattice.wire.msg.shared;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.type.StrV;

import java.util.List;

public record ParameterStatus(String key, String value) implements Message {
    @Override
    public char command() {
        return 'S';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("key", StrV::new),
                new MessageField<>("value", StrV::new)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { key(), value() };
    }
}
