package klattice.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public interface DomainFactory {
    static SqlParser createSqlParser(String sql) {
        return SqlParser.create(sql, sqlParserConfig());
    }

    static SqlParser.Config sqlParserConfig() {
        return SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setParserFactory(SqlParserImpl.FACTORY)
                .setUnquotedCasing(Casing.TO_UPPER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }
}
