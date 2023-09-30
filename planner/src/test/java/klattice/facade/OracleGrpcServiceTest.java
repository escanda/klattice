package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import klattice.calcite.SchemaHolder;
import klattice.exec.Exec;
import klattice.exec.SqlIdentifierResolver;
import klattice.grpc.OracleService;
import klattice.msg.Batch;
import klattice.msg.Environment;
import klattice.query.Querier;
import klattice.substrait.Shared;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class OracleGrpcServiceTest {
    @GrpcClient
    OracleService oracle;


    @Inject
    SqlIdentifierResolver resolver;

    @Inject
    Exec exec;

    @Test
    public void test_SimpleQuery() throws ValidationException, SqlParseException, RelConversionException {
        var batch = doQuery("SELECT 1");
        assertNotNull(batch);
        assertEquals(1, batch.getRowsList().size());
        assertEquals("1", batch.getRowsList().get(0).getFields(0).toStringUtf8());
    }

    @Test
    public void test_PgFunctionShapeSimpleQuery() throws ValidationException, SqlParseException, RelConversionException {
        var batch = doQuery("SELECT version()");
        assertNotNull(batch);
        assertEquals(1, batch.getRowsList().size());
        assertEquals("KLattice 1.0.0-SNAPSHOT", batch.getRowsList().get(0).getFields(0).toStringUtf8());
    }

    @Test
    public void test_PgFunctionShapeCompositeQuery() throws ValidationException, SqlParseException, RelConversionException {
        var batch = doQuery("select current_database() as a, current_schemas(false) as b");
        assertNotNull(batch);
        assertEquals(1, batch.getRowsList().size());
        assertEquals("KLattice 1.0.0-SNAPSHOT", batch.getRowsList().get(0).getFields(0).toStringUtf8());
    }

    @Test
    public void test_PgFunctionShapeCompositeQueryWithoutParens() throws ValidationException, SqlParseException, RelConversionException {
        var batch = doQuery("select current_database(), version(), current_user");
        assertNotNull(batch);
        assertEquals(1, batch.getRowsList().size());
        assertEquals("klattice", batch.getRowsList().get(0).getFields(0).toStringUtf8());
        assertEquals("public", batch.getRowsList().get(1).getFields(0).toStringUtf8());
        assertEquals("klattice_user", batch.getRowsList().get(1).getFields(0).toStringUtf8());
    }

    private Batch doQuery(String value) throws ValidationException, SqlParseException, RelConversionException {
        var environ = Environment.newBuilder().build();
        var schemaHolder = new SchemaHolder(environ);
        var querier = new Querier(schemaHolder);
        var sqlNode = querier.asSqlNode(value);
        var plan = querier.plan(sqlNode);
        var sql = Shared.toSql(resolver, environ, plan);
        return exec.execute(List.of(), sql.getSql());
    }
}
