package klattice.duckdb;

import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DuckDbService {
    private static final String DELIMITER = "\t";
    private static final String CMD_INSTALL_SUBSTRAIT = "INSTALL substrait";
    private static final String CMD_LOAD_SUBSTRAIT = "LOAD substrait";

    @LoggerName("DuckDbService")
    Logger logger;

    @RestClient
    DuckDbRestService duckDbRestService;
    private final AtomicInteger sessionId = new AtomicInteger(0);

    @Inject
    public void init() {
        ensureSessionId();
        doProvisioning();
    }

    protected void doProvisioning() {
        logger.infov("Provisioning Substrait extension to env...");
        duckDbRestService.execArbitrarySql(this.sessionId.get(), CMD_INSTALL_SUBSTRAIT);
        duckDbRestService.execArbitrarySql(this.sessionId.get(), CMD_LOAD_SUBSTRAIT);
        logger.infov("Installed Substrait extension to env");
    }

    protected void ensureSessionId() {
        var sessionId = duckDbRestService.makeSession();
        logger.infov("Got session id {0}", new Object[]{sessionId});
        this.sessionId.set(sessionId);
    }

    public Iterable<String[]> execSubstrait(byte[] payload) {
        try (var response = this.duckDbRestService.execSubstrait(this.sessionId.get(), payload)) {
            try (var is = response.readEntity(InputStream.class);
                 var bis = new BufferedReader(new InputStreamReader(is))) {
                return () -> bis.lines()
                        .map(line -> line.split(DELIMITER))
                        .iterator();
            } catch (IOException e) {
                logger.error("Error during reading response from substrait response");
                throw new RuntimeException(e);
            }
        }
    }

    public Iterable<String[]> execSql(String sql) {
        try (var response = this.duckDbRestService.execSqlQuery(this.sessionId.get(), sql)) {
            try (var is = response.readEntity(InputStream.class);
                 var bis = new BufferedReader(new InputStreamReader(is))) {
                return () -> bis.lines()
                        .map(line -> line.split(DELIMITER))
                        .iterator();
            } catch (IOException e) {
                logger.error("Error during reading response from sql response");
                throw new RuntimeException(e);
            }
        }
    }
}
