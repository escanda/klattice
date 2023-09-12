package klattice.calcite;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
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
        return List.of(FunctionDefs.VERSION.operator);
    }
}
