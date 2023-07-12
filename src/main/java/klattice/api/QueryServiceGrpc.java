package klattice.api;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import klattice.api.plan.Enhancer;
import klattice.api.plan.Parser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceGrpc implements Query {
    @Inject
    Parser parser;

    @Inject
    Enhancer enhancer;

    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Override
    public Uni<Plan> prepare(QueryDescriptor request) {
        try {
            var sql = parser.parse(request);
            var plan = enhancer.inflate(sql, request.getSourcesList());
            logger.info(plan);
            return Uni.createFrom().item(Plan.newBuilder().build());
        } catch (SqlParseException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }
}
