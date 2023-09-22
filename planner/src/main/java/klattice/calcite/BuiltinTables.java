package klattice.calcite;

import klattice.substrait.Shared;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public enum BuiltinTables {
    MAGIC_VALUES("_magic_values", new RelRecordType(StructKind.FULLY_QUALIFIED, List.of(
            new RelDataTypeFieldImpl(
                    "value",
                    0,
                    BasicSqlType.proto(SqlTypeName.VARCHAR, false)
                            .apply(Shared.relDataTypeFactory)
            ),
            new RelDataTypeFieldImpl(
                    "kind",
                    1,
                    BasicSqlType.proto(SqlTypeName.VARCHAR, false)
                            .apply(Shared.relDataTypeFactory)
            )
    )));

    public final String tableName;
    public final RelDataType rowType;

    BuiltinTables(String tableName, RelDataType rowType) {
        this.tableName = tableName;
        this.rowType = rowType;
    }
}
