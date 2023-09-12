package klattice.calcite;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;

import java.util.List;

@ApplicationScoped
public class SchemaInflator {
    public void enrichSchema(CalciteSchema rootSchema) {
        // TODO: custom types et al.
    }

    public SqlOperatorTable getSqlOperatorTable() {
        return SqlOperatorTables.of(getFunctionList());
    }

    private Iterable<? extends SqlOperator> getFunctionList() {
        return List.of(
                new SqlFunction(
                        FunctionNames.VERSION,
                        SqlKind.OTHER_FUNCTION,
                        ReturnTypes.VARCHAR,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION
                ));
    }
}
