package klattice.query;

import io.substrait.proto.Plan;
import klattice.calcite.SchemaHolder;
import klattice.substrait.CalciteToSubstraitConverter;
import klattice.substrait.Shared;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;

public class Querier {
    private final SchemaHolder schemaHolder;
    public final FrameworkConfig framework;
    public final Planner planner;

    public Querier(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
        framework = Shared.framework(this.schemaHolder);
        planner = Frameworks.getPlanner(framework);
    }

    public Plan plan(SqlNode sqlNode) throws SqlParseException, ValidationException, RelConversionException {
        var relNode = planner.rel(sqlNode);
        return CalciteToSubstraitConverter.getPlan(
                schemaHolder.getCatalog().getRootSchema(),
                schemaHolder.getTypeFactory(),
                relNode
        ).build();
    }

    public SqlNode asSqlNode(String query) throws SqlParseException, ValidationException {
        var sqlNode = planner.parse(query);
        return planner.validate(sqlNode);
    }
}
