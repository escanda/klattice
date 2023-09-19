package klattice.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class DuckDbDialect extends SqlDialect {
    public static DuckDbDialect INSTANCE = new DuckDbDialect();

    private DuckDbDialect() {
        super(DuckDbDialect.EMPTY_CONTEXT
                .withDatabaseProduct(DatabaseProduct.POSTGRESQL)
                .withConformance(SqlConformanceEnum.LENIENT));
    }
}
