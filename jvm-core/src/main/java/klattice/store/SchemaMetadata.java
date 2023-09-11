package klattice.store;

import klattice.row.RowTypeInfo;

public record SchemaMetadata(int id, String name, RowTypeInfo rowTypeInfo) {
}
