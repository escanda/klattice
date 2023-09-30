package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import klattice.calcite.SchemaHolder;
import klattice.exec.SqlIdentifierResolver;
import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Rel;
import klattice.msg.Schema;
import klattice.substrait.Shared;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class PrepareTest {
    @LoggerName("PrepareTest")
    Logger logger;

    @Test
    public void smokeTest() throws SqlParseException, RelConversionException, ValidationException {
        var type = Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build());
        var col = Column.newBuilder().setColumnName("public").setType(type.build());
        var projection = Rel.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col.build()))
                .build();
        var environ = Environment.newBuilder().addSchemas(Schema.newBuilder().setSchemaId(1).setRelName("PUBLIC").addRels(projection)).build();
        var q = "SELECT 'public' FROM PUBLIC.PUBLIC";
        var querier = new Querier(new SchemaHolder(environ));
        var sqlNode = querier.asSqlNode(q);
        var plan = querier.plan(sqlNode);
        logger.info(plan);
        assertNotNull(plan);
        assertTrue(plan.isInitialized());
    }

    @Test
    public void test_BuiltinFunctions() throws ValidationException, SqlParseException, RelConversionException {
        String q = "select VERSION(), CURRENT_DATABASE()";
        var environ = Environment.newBuilder();
        var querier = new Querier(new SchemaHolder(environ.build()));
        var sqlNode = querier.asSqlNode(q);
        var plan = querier.plan(sqlNode);
        var resolver = new SqlIdentifierResolver("http://localhost:8080");
        System.out.println(Shared.toSql(resolver, environ.build(), plan));
    }
}
