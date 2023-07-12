package klattice.api;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import picocli.CommandLine;

@QuarkusMain
@CommandLine.Command(name = "klattice-api", mixinStandardHelpOptions = true)
public class Main implements Runnable, QuarkusApplication {
    @Inject
    CommandLine.IFactory factory;

    @LoggerName("Main")
    Logger logger;

    @Override
    public int run(String... args) throws Exception {
        return new CommandLine(this, factory).execute(args);
    }

    @Override
    public void run() {
        logger.info("App up and running");
    }

    public static void main(String[] args) {
        Quarkus.run(Main.class, args);
    }
}
