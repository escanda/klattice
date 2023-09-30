package klattice.exec;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import klattice.calcite.SchemaHolder;
import klattice.grpc.ExecService;
import klattice.msg.Environment;
import klattice.msg.ExpandedPlan;
import klattice.msg.SqlStatements;
import klattice.query.Querier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ExecGrpcServiceTest {
    @GrpcClient
    ExecService execService;

    @Test
    public void test_smoke() throws ValidationException, SqlParseException {
        var querier = new Querier(new SchemaHolder(Environment.newBuilder().build()));
        var sqlNode = querier.asSqlNode("SELECT 1");
        var sqlStatements = SqlStatements.newBuilder().addSqlStatement(sqlNode.toString());
        var expandedPlan = ExpandedPlan.newBuilder().setSqlStatements(sqlStatements).build();
        var batch = execService.execute(expandedPlan)
                .await()
                .indefinitely();
        System.out.println(batch);
    }
}
