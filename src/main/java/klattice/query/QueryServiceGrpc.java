package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import klattice.msg.PreparedQuery;
import klattice.msg.QueryDescriptor;
import klattice.msg.QueryDiagnostics;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceGrpc implements Query {
    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Override
    public Uni<PreparedQuery> prepare(QueryDescriptor request) {
        var prepare = new Prepare();
        PreparedQuery preparedQuery = null;
        try {
            preparedQuery = prepare.compile(request.getQuery(), request.getSourcesList());
        } catch (SqlParseException e) {
            logger.warnv("Error parsing statement {0} with error", new Object[]{request.getQuery()}, e);
            preparedQuery = PreparedQuery.newBuilder().setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build()).build();
        }
        logger.infov("Query {0} became {1} prepared query", request, preparedQuery);
        return Uni.createFrom().item(preparedQuery);
    }
}
