package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.calcite.DomainFactory;
import klattice.calcite.SchemaInflator;
import klattice.msg.Plan;
import klattice.msg.PreparedQuery;
import klattice.msg.QueryDescriptor;
import klattice.msg.QueryDiagnostics;
import klattice.schema.SchemaFactory;
import klattice.substrait.CalciteToSubstraitConverter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.jboss.logging.Logger;

@GrpcService
public class QueryServiceGrpc implements Query {
    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Inject
    SchemaInflator schemaInflator;

    @Blocking
    @Override
    public Uni<PreparedQuery> inflate(QueryDescriptor request) {
        PreparedQuery preparedQuery;
        try {
            var inspector = new SchemaFactory(schemaInflator, request.getEnviron());
            var planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                    .parserConfig(DomainFactory.sqlParserConfig())
                    .defaultSchema(inspector.getCatalog().getRootSchema().plus())
                    .operatorTable(schemaInflator.getSqlOperatorTable())
                    .build());
            var sqlNode = planner.parse(request.getQuery());
            var rewrittenSqlNode = planner.validate(sqlNode);
            var relNode = planner.rel(rewrittenSqlNode);
            var plan = CalciteToSubstraitConverter.getPlan(relNode);
            preparedQuery = PreparedQuery.newBuilder().setPlan(Plan.newBuilder().setEnviron(request.getEnviron()).setPlan(plan).build()).build();
        } catch (SqlParseException | RelConversionException | ValidationException e) {
            logger.warnv("Error preparing statement {0} with error {1}", request.getQuery(), e);
            preparedQuery = PreparedQuery.newBuilder().setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build()).build();
        }
        logger.infov("Query {0} became {1}", request, preparedQuery);
        return Uni.createFrom().item(preparedQuery);
    }
}
