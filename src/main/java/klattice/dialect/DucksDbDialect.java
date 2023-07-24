package klattice.dialect;

import org.apache.calcite.sql.SqlDialect;

public class DucksDbDialect extends SqlDialect {
    public static DucksDbDialect INSTANCE = new DucksDbDialect();

    private DucksDbDialect() {
        super(DucksDbDialect.EMPTY_CONTEXT
                .withDatabaseProduct(DatabaseProduct.POSTGRESQL));
    }
}
