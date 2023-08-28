package klattice.calcite;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

@ApplicationScoped
public class DomainFactory {
    public SqlParser createSqlParser(String sql) {
        return SqlParser.create(sql, sqlParserConfig());
    }

    public SqlParser.Config sqlParserConfig() {
        return SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setParserFactory(SqlParserImpl.FACTORY)
                .setUnquotedCasing(Casing.TO_UPPER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }
}
