package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import klattice.msg.Query;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class OracleGrpcServiceTest {
    @GrpcClient
    Oracle oracle;

    @Test
    public void test_SimpleQuery() {
        var batch = oracle.answer(Query.newBuilder().setQuery("SELECT 1").build())
                .await()
                .indefinitely();
        System.out.println(batch);
        assertNotNull(batch);
        assertEquals(1, batch.getRowsList().size());
    }
}
