package klattice.exec;

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
        duckDbRestService.execArbitrarySql(this.sessionId.get(), "INSTALL substrait");
        duckDbRestService.execArbitrarySql(this.sessionId.get(), "LOAD substrait");
        logger.infov("Installed Substrait extension to env");
    }

    protected void ensureSessionId() {
        var sessionId = duckDbRestService.makeSession();
        logger.infov("Got session id {0}", new Object[]{sessionId});
        this.sessionId.set(sessionId);
    }

    public Iterable<String[]> execSubstrait(byte[] payload) {
        try (var response = this.duckDbRestService.execSubstrait(this.sessionId.get(), payload)) {
            if (!(response.getStatus() >= 200 && response.getStatus() < 300)) {
                logger.warn("Response code from server was " + response.getStatus());
            }
            try (var is = response.readEntity(InputStream.class);
                 var bis = new BufferedReader(new InputStreamReader(is))) {
                return () -> bis.lines()
                        .map(line -> line.split(DELIMITER))
                        .iterator();
            } catch (IOException e) {
                logger.error("Error during reading IO from substrait response");
                throw new RuntimeException(e);
            }
        }
    }
}
