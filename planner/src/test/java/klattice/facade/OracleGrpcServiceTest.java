package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import klattice.grpc.OracleService;
import klattice.msg.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class OracleGrpcServiceTest {
    @GrpcClient
    OracleService oracle;

    @Test
    public void test_SimpleQuery() {
        var batch = oracle.answer(Query.newBuilder().setQuery("SELECT 1").build())
                .await()
                .indefinitely();
        assertNotNull(batch);
        Assertions.assertEquals(1, batch.getRowsList().size());
        Assertions.assertEquals("1", batch.getRowsList().get(0).getFields(0).toStringUtf8());
    }
}
