package klattice.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public enum BuiltinTables {
    ZERO("zero", new RelRecordType(List.of(
            new RelDataTypeFieldImpl("value", 0, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR))
    )));

    public final String tableName;
    public final RelDataType rowType;

    BuiltinTables(String tableName, RelDataType rowType) {
        this.tableName = tableName;
        this.rowType = rowType;
    }
}
