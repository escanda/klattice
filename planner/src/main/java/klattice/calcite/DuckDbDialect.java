package klattice.calcite;

import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class DuckDbDialect extends PostgresqlSqlDialect {
    public static DuckDbDialect INSTANCE = new DuckDbDialect();

    private DuckDbDialect() {
        super(PostgresqlSqlDialect.DEFAULT_CONTEXT);
    }
}
