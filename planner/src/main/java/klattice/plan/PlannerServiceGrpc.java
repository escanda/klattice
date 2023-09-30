package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.substrait.plan.ProtoPlanConverter;
import jakarta.inject.Inject;
import klattice.calcite.DuckDbDialect;
import klattice.calcite.SchemaHolder;
import klattice.exec.SqlIdentifierResolver;
import klattice.grpc.PlannerService;
import klattice.msg.ExpandedPlan;
import klattice.msg.PlanDiagnostics;
import klattice.msg.SqlStatements;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jboss.logging.Logger;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@GrpcService
public class PlannerServiceGrpc implements PlannerService {
    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Inject
    SqlIdentifierResolver resolver;

    @Blocking
    @Override
    public Uni<klattice.msg.ExpandedPlan> expand(klattice.msg.Plan request) {
        var environ = request.getEnviron();
        var reqPlan = request.getPlan();
        var relPlan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(reqPlan);
        var relRoots = SubstraitToCalciteConverter.getRelRoots(relPlan);
        if (relRoots.isEmpty()) {
            var errorMessage = "Empty plans";
            logger.warn(errorMessage);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(errorMessage).build()).build());
        } else {
            var schemaFactory = new SchemaHolder(environ);
            var optimizer = new Optimizer(schemaFactory);
            var rewrittenNodes = optimizer.relnodes(relRoots);
            var relToSqlConverter = new RelToSqlConverter(DuckDbDialect.INSTANCE);
            var sqlStatements = rewrittenNodes.stream().map(relToSqlConverter::visitRoot).map(SqlImplementor.Result::asSelect).map(sqlSelect -> {
                var sqlNode = sqlSelect.accept(new SqlShuttle() {
                    @Override
                    public @Nullable SqlNode visit(SqlIdentifier id) {
                        return resolver.resolve(environ, id)
                                .map(translatedIdRef -> new SqlIdentifier(translatedIdRef.url(), id.getCollation(), id.getParserPosition()))
                                .orElse(id);
                    }
                });
                assert sqlNode != null;
                var sb = new StringBuilder(2048);
                var sqlWriter = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(DuckDbDialect.INSTANCE), sb);
                sqlNode.unparse(sqlWriter, 0, 0);
                return sb.toString();
            }).toList();
            logger.infov("Original plan was:\n {0} \nConverted into SQL:\n {1}", request.getPlan(), sqlStatements);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(false).setSqlStatements(SqlStatements.newBuilder().addAllSqlStatement(sqlStatements)).build());
        }
    }
}
