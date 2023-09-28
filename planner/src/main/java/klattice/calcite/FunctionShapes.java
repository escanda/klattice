package klattice.calcite;

import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public enum FunctionShapes {
    VERSION(FunctionCategory.MAGIC, SqlBasicFunction.create(
            FunctionNames.VERSION,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.VERSION),
    CURRENT_DATABASE(FunctionCategory.MAGIC, SqlBasicFunction.create(
            FunctionNames.CURRENT_DATABASE,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_DATABASE),
    CURRENT_SCHEMAS(FunctionCategory.SCHEMAS, SqlBasicFunction.create(
            FunctionNames.CURRENT_SCHEMAS,
            ReturnTypes.TO_ARRAY,
            OperandTypes.NILADIC.or(OperandTypes.BOOLEAN),
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_SCHEMAS),
    CURRENT_USER(FunctionCategory.MAGIC, SqlBasicFunction.create(
            FunctionNames.CURRENT_USER,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_USER),
    CURRENT_SCHEMA(FunctionCategory.MAGIC, SqlBasicFunction.create(
            FunctionNames.CURRENT_SCHEMA,
            ReturnTypes.VARCHAR,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
    ), FunctionNames.CURRENT_SCHEMA),
    ;

    public final FunctionCategory category;
    public final SqlOperator operator;
    public final String discriminator;

    FunctionShapes(FunctionCategory category, SqlOperator operator, String discriminator) {
        this.category = category;
        this.operator = operator;
        this.discriminator = discriminator;
    }
}
