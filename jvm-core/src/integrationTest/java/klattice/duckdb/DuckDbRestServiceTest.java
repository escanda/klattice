package klattice.duckdb;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusIntegrationTest
public class DuckDbRestServiceTest {
    @RestClient
    DuckDbRestService duckDbRestService;

    @Test
    public void testSessionUpsert() {
        try (var response = duckDbRestService.makeSession(1)) {
            assertEquals(200, response.getStatus());
            var sessionId = response.readEntity(Integer.class);
            assertNotEquals(-1, sessionId, "wrong session id");
            assertTrue(sessionId < 1000, "session id overflow");
        }
    }

    @Test
    public void testMakeSession() {
        var sessionId = duckDbRestService.makeSession();
        System.out.println(sessionId);
    }

    @Test
    public void test_substraitProvisioning() {
        var sessionId = duckDbRestService.makeSession();
        duckDbRestService.execArbitrarySql(sessionId, "INSTALL substrait");
    }
}
