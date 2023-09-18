package klattice.wire.msg.server;

import klattice.wire.msg.*;
import klattice.wire.msg.type.StrV;

import java.util.List;

public record CommandComplete(int rows, Tag tag) implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.CommandComplete.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("commandTag", StrV::new)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { tag().format(rows()) };
    }

    public enum Tag {
        INSERT(true),
        DELETE(true),
        UPDATE(true),
        MERGE(true),
        SELECT(true),
        MOVE(true),
        FETCH(true),
        COPY(true),
        SET(false);

        public final boolean hasRowCount;

        Tag(boolean hasRowCount) {
            this.hasRowCount = hasRowCount;
        }

        public String format(int rows) {
            if (this == INSERT) {
                return name() + " 0 " + rows;
            } else if (hasRowCount) {
                return name() + " " + rows;
            } else {
                return name();
            }
        }
    }
}
