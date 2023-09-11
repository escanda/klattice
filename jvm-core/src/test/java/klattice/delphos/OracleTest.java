package klattice.delphos;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class OracleTest {
    @Inject
    Oracle oracle;

    @Test
    public void test_SimpleQuery() {
        System.out.println(oracle.answer("SELECT version()").await().indefinitely());
    }
}
