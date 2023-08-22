package klattice;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import klattice.exec.DuckDbService;
import klattice.msg.Environment;
import klattice.msg.PreparedQuery;
import klattice.query.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

@QuarkusMain
@CommandLine.Command(name = "klattice-api", mixinStandardHelpOptions = true)
public class Main implements Runnable, QuarkusApplication {
    @Inject
    CommandLine.IFactory factory;

    @LoggerName("Main")
    Logger logger;

    @ConfigProperty(name = "quarkus.grpc.server.host")
    String host;

    @ConfigProperty(name = "quarkus.grpc.server.port")
    String port;

    @Inject
    DuckDbService duckDbService;

    @Override
    public int run(String... args) {
        return new CommandLine(this, factory).execute(args);
    }

    @Override
    public void run() {
        logger.infov("App up and running in {0}:{1}", host, port);

        PreparedQuery preparedQuery;
        try {
            preparedQuery = new Prepare().compile("SELECT 1 + 1", Environment.newBuilder().build());
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            logger.error("cannot create substrait payload", e);
            throw new RuntimeException(e);
        }
        var output = new ByteArrayOutputStream();
        try {
            preparedQuery.getPlan().writeTo(output);
        } catch (IOException e) {
            logger.error("Cannot write substrait plan to bytes", e);
            throw new RuntimeException(e);
        }
        var colIterable = duckDbService.execSubstrait(output.toByteArray());
        for (String[] columns : colIterable) {
            System.out.println(Arrays.toString(columns));
        }
        Quarkus.waitForExit();
    }

    public static void main(String[] args) {
        Quarkus.run(Main.class, args);
    }
}
