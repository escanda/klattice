package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.type.Int16V;
import klattice.wire.msg.type.Int32V;
import klattice.wire.msg.type.VarByteV;

import java.util.ArrayList;
import java.util.Collection;

public record DataRow(Collection<Column> columns) implements Message {
    @Override
    public char command() {
        return 'D';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> {
            var cols = new ArrayList<MessageField<?>>();
            cols.add(new MessageField<>("colNo", Int16V::new));
            for (int i = 0; i < columns().size(); i++) {
                cols.add(new MessageField<>("colLength", Int32V::new));
                cols.add(new MessageField<>("colValue", VarByteV::new));
            }
            return new PgsqlPayload(command(), cols);
        };
    }

    @Override
    public Object[] values() {
        var values = new ArrayList<>();
        values.add((short) columns.size());
        for (Column column : columns) {
            values.add(column.value.length);
            values.add(column.value);
        }
        return values.toArray();
    }

    public record Column(int colWidth, byte[] value) {}
}
