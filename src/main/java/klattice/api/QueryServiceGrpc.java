package klattice.api;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import klattice.api.plan.Parser;
import klattice.api.plan.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceGrpc implements Query {
    @Inject
    Parser parser;

    @Inject
    Prepare enhancer;

    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Override
    public Uni<PreparedQuery> prepare(QueryDescriptor request) {
        try {
            var sql = parser.parse(request);
            var plan = enhancer.inflate(sql, request.getSourcesList());
            logger.info(plan);
            return Uni.createFrom().item(PreparedQuery.newBuilder().setPlan(plan).build());
        } catch (SqlParseException e) {
            logger.error(e);
            var pq = PreparedQuery.newBuilder().setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build());
            return Uni.createFrom().item(pq.build());
        }
    }
}
