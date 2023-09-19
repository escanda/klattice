package klattice.calcite;

import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public enum FunctionDefs {
    VERSION(FunctionCategory.MAGIC, SqlBasicFunction.create(
            FunctionNames.VERSION,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.VERSION),
    COALESCE(FunctionCategory.EQUIVALENCE, SqlStdOperatorTable.COALESCE, FunctionNames.COALESCE);

    public final FunctionCategory category;
    public final SqlOperator operator;
    public final String discriminator;

    FunctionDefs(FunctionCategory category, SqlOperator operator, String discriminator) {
        this.category = category;
        this.operator = operator;
        this.discriminator = discriminator;
    }
}
