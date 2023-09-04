package klattice.wire.msg.server;

import klattice.wire.msg.Message;
import klattice.wire.msg.MessageField;
import klattice.wire.msg.PgsqlPayload;
import klattice.wire.msg.PgsqlPayloadProvider;
import klattice.wire.msg.type.SingleByteV;

import java.util.List;

public record ReadyForQuery(TransactionStatus transactionStatus) implements Message {
    @Override
    public char command() {
        return 'Z';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> new PgsqlPayload(command(), List.of(
                new MessageField<>("transactionIndicator", () -> SingleByteV.I)
        ));
    }

    @Override
    public Object[] values() {
        return new Object[] { transactionStatus().value };
    }

    public enum TransactionStatus {
        IDLE('I'),
        STARTED('T'),
        FAILED('E');

        public final char value;

        TransactionStatus(char value) {
            this.value = value;
        }
    }
}
