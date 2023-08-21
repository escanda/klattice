package klattice.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class DucksDbDialect extends SqlDialect {
    public static DucksDbDialect INSTANCE = new DucksDbDialect();

    private DucksDbDialect() {
        super(DucksDbDialect.EMPTY_CONTEXT
                .withDatabaseProduct(DatabaseProduct.POSTGRESQL)
                .withConformance(SqlConformanceEnum.LENIENT));
    }
}
