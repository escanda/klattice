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
    ), FunctionNames.VERSION);

    public final SqlOperator operator;
    public final String discriminator;

    FunctionDefs(SqlOperator operator, String discriminator) {
        this.operator = operator;
        this.discriminator = discriminator;
    }
}
