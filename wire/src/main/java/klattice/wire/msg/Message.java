package klattice.wire.msg;

import klattice.wire.msg.type.Int32V;

import java.util.List;
import java.util.function.Supplier;

public interface Message {
    char command();
    PgsqlPayloadProvider provider();
    Object[] values();

    default PgsqlPayloadProvider emptyProvider() {
        return () -> new PgsqlPayload(command(), List.of());
    }

    default Supplier<? extends PgsqlValueType<Integer>> signedInt() {
        return Int32V::new;
    }
}
