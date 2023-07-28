package klattice.query;

import jakarta.enterprise.context.Dependent;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.plan.Converter;
import klattice.schema.SchemaDescriptorFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;

@Dependent
public class Prepare {
    public PreparedQuery compile(String query, List<Environment> environments) throws SqlParseException {
        var parser = SqlParser.create(query);
        var sql = parser.parseQuery();
        var inspector = new SchemaDescriptorFactory(environments);
        var validator = inspector.getSqlValidator();
        var sqlToRelConverter = new SqlToRelConverter(
                null,
                validator,
                inspector.getCatalog(),
                inspector.getRelOptCluster(),
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
        var relNode = sqlToRelConverter.convertQuery(sql, true, true);
        var plan = Converter.getPlan(relNode);
        return PreparedQuery.newBuilder().setPlan(Plan.newBuilder().addAllEnviron(environments).setPlan(plan).build()).build();
    }
}
