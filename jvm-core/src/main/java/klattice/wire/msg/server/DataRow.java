package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.type.Int16V;

import java.util.Collection;
import java.util.List;

public record DataRow(short colNo, Collection<Column> columns) implements Message {
    @Override
    public char command() {
        return 'D';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("colNo", Int16V::new),
                new MessageField<>("colWidth", Int16V::new)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[0];
    }

    public record Column(int colWidth, byte[] value) {}
}
