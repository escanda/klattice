package klattice.api;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import org.jboss.logging.Logger;

import static io.quarkus.logging.Log.log;

public class Main implements QuarkusApplication {
    public static void main(String[] args) {
        Quarkus.run(Main.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        log(Logger.Level.INFO, "Running Query service...");
        return 0;
    }

}
