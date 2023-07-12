package klattice.api;

import io.quarkus.arc.log.LoggerName;
import io.substrait.proto.Type;
import klattice.api.plan.Enhancer;
import klattice.api.plan.Parser;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;

import java.util.List;

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
            var projectionBuilder = RelDescriptor.newBuilder().addColumnName("public").addTyping(Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build()).build());
            var schemaSourceDetails1Builder = SchemaDescriptor.newBuilder().addProjections(projectionBuilder.build());
            var schemaSourcesList = List.of(schemaSourceDetails1Builder.build());
            var plan = enhancer.inflate(sql, schemaSourcesList);
            logger.info(plan);
            return Uni.createFrom().item(Plan.newBuilder().build());
        } catch (SqlParseException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }
}
