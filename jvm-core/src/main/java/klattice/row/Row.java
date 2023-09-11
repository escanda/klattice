package klattice.row;

import java.util.function.Supplier;

public record Row(Object[] values, Supplier<RowTypeInfo> rowTypeInfoSupplier) {}
