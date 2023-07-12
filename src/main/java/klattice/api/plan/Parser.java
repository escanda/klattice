package klattice.api.plan;

import jakarta.enterprise.context.Dependent;
import klattice.api.QueryDescriptor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

@Dependent
public class Parser {
    public SqlNode parse(QueryDescriptor context) throws SqlParseException {
        var q = context.getQuery();
        var parser = SqlParser.create(q);
        return parser.parseQuery();
    }
}
