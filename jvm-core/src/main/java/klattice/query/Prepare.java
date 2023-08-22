package klattice.query;

import jakarta.enterprise.context.Dependent;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.plan.Converter;
import klattice.schema.SchemaFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

@Dependent
public class Prepare {
    public PreparedQuery compile(String query, Environment environ) throws SqlParseException, RelConversionException, ValidationException {
        var inspector = new SchemaFactory(environ);
        var planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                        .defaultSchema(inspector.getCatalog().getRootSchema().plus())
                .build());
        var sqlNode = planner.parse(query);
        var rewrittenSqlNode = planner.validate(sqlNode);
        var relNode = planner.rel(rewrittenSqlNode);
        var plan = Converter.getPlan(relNode);
        return PreparedQuery.newBuilder().setPlan(Plan.newBuilder().setEnviron(environ).setPlan(plan).build()).build();
    }
}
