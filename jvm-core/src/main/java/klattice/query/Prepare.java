package klattice.query;

import jakarta.enterprise.context.Dependent;
import klattice.calcite.DomainFactory;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.registry.SchemaFactory;
import klattice.substrait.CalciteToSubstraitConverter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

@Dependent
public class Prepare {
    public PreparedQuery compile(String query, Environment environ) throws SqlParseException, RelConversionException, ValidationException {
        var inspector = new SchemaFactory(environ);
        var planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                        .parserConfig(DomainFactory.sqlParserConfig())
                        .defaultSchema(inspector.getCatalog().getRootSchema().plus())
                .build());
        var sqlNode = planner.parse(query);
        var rewrittenSqlNode = planner.validate(sqlNode);
        var relNode = planner.rel(rewrittenSqlNode);
        var plan = CalciteToSubstraitConverter.getPlan(relNode);
        return PreparedQuery.newBuilder().setPlan(Plan.newBuilder().setEnviron(environ).setPlan(plan).build()).build();
    }
}
