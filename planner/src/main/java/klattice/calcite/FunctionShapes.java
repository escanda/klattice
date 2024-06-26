package klattice.calcite;

import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public enum FunctionShapes {
    VERSION(SqlBasicFunction.create(
            FunctionNames.VERSION,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.VERSION),
    CURRENT_DATABASE(SqlBasicFunction.create(
            FunctionNames.CURRENT_DATABASE,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_DATABASE),
    CURRENT_SCHEMAS(SqlBasicFunction.create(
            FunctionNames.CURRENT_SCHEMAS,
            ReturnTypes.TO_ARRAY,
            OperandTypes.NILADIC.or(OperandTypes.BOOLEAN),
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_SCHEMAS),
    CURRENT_USER(SqlBasicFunction.create(
            FunctionNames.CURRENT_USER,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.SYSTEM
    ), FunctionNames.CURRENT_USER),
    CURRENT_SCHEMA(SqlBasicFunction.create(
            FunctionNames.CURRENT_SCHEMA,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_SCHEMA),
    ;

    public final SqlOperator operator;
    public final String discriminator;

    FunctionShapes(SqlOperator operator, String discriminator) {
        this.operator = operator;
        this.discriminator = discriminator;
    }
}
