package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import klattice.calcite.SchemaHolder;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.msg.QueryDescriptor;
import klattice.msg.QueryDiagnostics;
import klattice.substrait.CalciteToSubstraitConverter;
import klattice.substrait.Shared;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
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
        PreparedQuery preparedQuery;
        try {
            var schemaFactory = new SchemaHolder(request.getEnviron());
            var framework = Shared.framework(schemaFactory);
            var planner = Frameworks.getPlanner(framework);
            var sqlNode = planner.parse(request.getQuery());
            var rewrittenSqlNode = planner.validate(sqlNode);
            var relNode = planner.rel(rewrittenSqlNode);
            var plan = CalciteToSubstraitConverter.getPlan(schemaFactory.getCatalog().getRootSchema(), schemaFactory.getTypeFactory(), relNode);
            preparedQuery = PreparedQuery.newBuilder().setPlan(Plan.newBuilder().setEnviron(request.getEnviron()).setPlan(plan).build()).build();
        } catch (SqlParseException | RelConversionException | ValidationException e) {
            logger.warnv("Error preparing statement {0} with error {1}", request.getQuery(), e);
            preparedQuery = PreparedQuery.newBuilder().setHasError(true).setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build()).build();
        }
        logger.infov("Query {0} became {1}", request, preparedQuery);
        return Uni.createFrom().item(preparedQuery);
    }
}
