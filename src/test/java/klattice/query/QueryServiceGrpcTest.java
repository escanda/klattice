package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.substrait.proto.Type;
import klattice.msg.QueryDescriptor;
import klattice.msg.RelDescriptor;
import klattice.msg.SchemaDescriptor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusIntegrationTest
public class QueryServiceGrpcTest {
    @GrpcClient
    Query query;

    @LoggerName("QueryServiceGrpcTest")
    Logger logger;

    @Test
    public void smokeTest() throws SqlParseException {
        var projection = RelDescriptor.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addTyping(Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build()))
                .build();
        var schemaSources = List.of(SchemaDescriptor.newBuilder().setSchemaId(1).setRelName("PUBLIC").addProjections(projection).build());

        var q = "SELECT public FROM PUBLIC.PUBLIC";
        var qc = QueryDescriptor.newBuilder()
                .setQuery(q)
                .addAllSources(schemaSources)
                .build();
        var preparedQuery = query.prepare(qc).await().indefinitely();
        logger.info(preparedQuery);
        assertNotNull(preparedQuery);
    }
}
