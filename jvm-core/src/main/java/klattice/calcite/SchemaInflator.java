package klattice.calcite;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;

import java.util.List;

@ApplicationScoped
public class SchemaInflator {
    public void enrichSchema(CalciteSchema rootSchema) {
        // TODO: custom types et al.
    }

    public SqlOperatorTable getSqlOperatorTable() {
        return SqlOperatorTables.of(getFunctionOperators());
    }

    private Iterable<? extends SqlOperator> getFunctionOperators() {
        return List.of(
                SqlBasicFunction.create(
                        FunctionNames.VERSION,
                        ReturnTypes.CURSOR,
                        OperandTypes.NILADIC,
                        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
                )
        );
    }
}
