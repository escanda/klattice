package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import klattice.msg.Batch;
import klattice.msg.PreparedQuery;
import klattice.msg.QueryDescriptor;
import klattice.msg.QueryDiagnostics;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceGrpc implements Query {
    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Blocking
    @Override
    public Uni<PreparedQuery> inflate(QueryDescriptor request) {
        var prepare = new Prepare();
        PreparedQuery preparedQuery;
        try {
            preparedQuery = prepare.compile(request.getQuery(), request.getEnviron());
        } catch (SqlParseException | RelConversionException | ValidationException e) {
            logger.warnv("Error preparing statement {0} with error {1}", new Object[]{request.getQuery()}, e);
            preparedQuery = PreparedQuery.newBuilder().setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build()).build();
        }
        logger.infov("Query {0} became {1} prepared query", request, preparedQuery);
        return Uni.createFrom().item(preparedQuery);
    }
}
