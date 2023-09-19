package klattice.calcite;

import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public enum FunctionDefs {
    VERSION(SqlBasicFunction.create(
            FunctionNames.VERSION,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ));

    public final SqlOperator operator;

    FunctionDefs(SqlOperator operator) {
        this.operator = operator;
    }

    public static final String MAGIC_TABLE = "zero";
}
