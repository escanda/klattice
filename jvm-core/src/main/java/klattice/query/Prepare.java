package klattice.query;

import jakarta.enterprise.context.Dependent;
import klattice.calcite.DomainFactory;
import klattice.calcite.SchemaInflator;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.schema.SchemaFactory;
import klattice.substrait.CalciteToSubstraitConverter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

@Dependent
public class Prepare {
    SchemaInflator schemaInflator = new SchemaInflator();

    public PreparedQuery compile(String query, Environment environ) throws SqlParseException, RelConversionException, ValidationException {
        var inspector = new SchemaFactory(schemaInflator, environ);
        var planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                        .parserConfig(DomainFactory.sqlParserConfig())
                        .defaultSchema(inspector.getCatalog().getRootSchema().plus())
                        .operatorTable(schemaInflator.getSqlOperatorTable())
                .build());
        var sqlNode = planner.parse(query);
        var rewrittenSqlNode = planner.validate(sqlNode);
        var relNode = planner.rel(rewrittenSqlNode);
        var plan = CalciteToSubstraitConverter.getPlan(relNode);
        return PreparedQuery.newBuilder().setPlan(Plan.newBuilder().setEnviron(environ).setPlan(plan).build()).build();
    }
}
