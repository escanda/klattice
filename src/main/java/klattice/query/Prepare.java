package klattice.query;

import jakarta.enterprise.context.Dependent;
import klattice.msg.PlanDescriptor;
import klattice.msg.PreparedQuery;
import klattice.msg.SchemaDescriptor;
import klattice.plan.Converter;
import klattice.schema.SchemaDescriptorInspector;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;

@Dependent
public class Prepare {
    public PreparedQuery compile(String query, List<SchemaDescriptor> sources) throws SqlParseException {
        var parser = SqlParser.create(query);
        var sql = parser.parseQuery();
        var inspector = new SchemaDescriptorInspector(sources);
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
        return PreparedQuery.newBuilder().setPlan(PlanDescriptor.newBuilder().addAllSources(sources).setPlan(plan).build()).build();
    }
}
