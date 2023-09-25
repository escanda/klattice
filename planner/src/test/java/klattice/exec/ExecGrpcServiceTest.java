package klattice.exec;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import klattice.calcite.SchemaHolder;
import klattice.grpc.ExecService;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.query.Querier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ExecGrpcServiceTest {
    @GrpcClient
    ExecService execService;

    @Test
    public void test_smoke() throws ValidationException, SqlParseException, RelConversionException {
        var querier = new Querier(new SchemaHolder(Environment.newBuilder().build()));
        var plan = querier.plan("SELECT 1");
        var batch = execService.execute(Plan.newBuilder().setPlan(plan).build())
                .await()
                .indefinitely();
        System.out.println(batch);
    }
}
