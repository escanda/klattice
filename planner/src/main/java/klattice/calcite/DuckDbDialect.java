package klattice.calcite;

import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class DuckDbDialect extends PostgresqlSqlDialect {
    public static final Context CONTEXT = DEFAULT_CONTEXT.withConformance(SqlConformanceEnum.LENIENT);
    public static DuckDbDialect INSTANCE = new DuckDbDialect();

    private DuckDbDialect() {
        super(CONTEXT);
    }
}
