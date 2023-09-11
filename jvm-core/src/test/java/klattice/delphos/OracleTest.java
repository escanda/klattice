package klattice.delphos;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class OracleTest {
    @Inject
    Oracle oracle;

    @Test
    public void test_SimpleQuery() {
        var batch = oracle.answer("SELECT version()").await().indefinitely();
        System.out.println(batch);
        assertNotNull(batch);
    }
}
