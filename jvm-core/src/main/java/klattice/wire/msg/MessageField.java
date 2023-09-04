package klattice.wire.msg;

import java.util.function.Supplier;

public record MessageField<T>(String name, Supplier<? extends PgsqlValueType<T>> supplier) {
}
