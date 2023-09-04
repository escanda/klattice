package klattice.wire.msg;

public record ValueRec<T>(Object value, PgsqlValueType<T> valueType) {}
