package klattice.api;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.api.plan.Enhancer;
import klattice.api.plan.Parser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import picocli.CommandLine;

@QuarkusMain
@CommandLine.Command(name = "klattice-api", mixinStandardHelpOptions = true)
public class Main implements Runnable, QuarkusApplication {
    @Inject
    CommandLine.IFactory factory;

    @CommandLine.Option(names = "-p", description = "Prepares SQL and dumps Substrait in JSON")
    private String sql;

    @CommandLine.Option(names = "-sH", description = "Schema host")
    private String host;

    @CommandLine.Option(names = "-sP", description = "Schema port")
    private short port;

    @CommandLine.Option(names = "-sid", description = "Schema ID")
    private long schemaId;

    @Override
    public int run(String... args) throws Exception {
        return new CommandLine(this, factory).execute(args);
    }

    @Override
    public void run() {
        var projection = RelDescriptor.newBuilder().addColumnName("public").addTyping(Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build()).build());
        var schema = QueryDescriptor.newBuilder();
        schema = schema.addSources(SchemaDescriptor.newBuilder()
                .setSchemaId(1L)
                .addProjections(projection)
                .build()
        );
        sql = "SELECT * FROM public";
        schema = schema.setQuery(sql);
        try {
            var qc = schema.build();
            var enhancer = new Enhancer();
            var parser = new Parser();
            var sqlNode = parser.parse(qc);
            var plan = enhancer.inflate(sqlNode, qc.getSourcesList());
            System.out.println(plan);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Quarkus.run(Main.class, args);
    }
}
