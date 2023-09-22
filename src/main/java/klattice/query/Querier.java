package klattice.query;

import io.substrait.proto.Plan;
import klattice.calcite.SchemaHolder;
import klattice.substrait.CalciteToSubstraitConverter;
import klattice.substrait.Shared;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class Querier {
    private final SchemaHolder schemaHolder;

    public Querier(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    public Plan plan(String query) throws SqlParseException, ValidationException, RelConversionException {
        var framework = Shared.framework(schemaHolder);
        var planner = Frameworks.getPlanner(framework);
        var sqlNode = planner.parse(query);
        var rewrittenSqlNode = planner.validate(sqlNode);
        var relNode = planner.rel(rewrittenSqlNode);
        return CalciteToSubstraitConverter.getPlan(
                schemaHolder.getCatalog().getRootSchema(),
                schemaHolder.getTypeFactory(),
                relNode
        ).build();
    }
}
