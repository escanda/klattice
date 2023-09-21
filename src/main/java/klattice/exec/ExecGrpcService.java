package klattice.exec;

import com.google.protobuf.ByteString;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.substrait.plan.ProtoPlanConverter;
import jakarta.inject.Inject;
import klattice.calcite.DuckDbDialect;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Plan;
import klattice.msg.Row;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@GrpcService
public class ExecGrpcService implements Exec {
    @LoggerName("ExecGrpcService")
    Logger logger;

    @Inject
    SqlIdentifierResolver sqlIdentifierResolver;

    @Inject
    DuckDbService duckDbService;

    @Blocking
    @Override
    public Uni<Batch> execute(Plan request) {
        var plan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(request.getPlan());
        var relRoots = SubstraitToCalciteConverter.getRelRoots(plan);
        var sqlStmts = relRoots.stream().map(relNode -> new RelToSqlConverter(DuckDbDialect.INSTANCE).visitRoot(relNode).asSelect()).toList();
        var sql = sqlStmts.stream().findFirst().orElseThrow();
        var sqlNode = sql.accept(new SqlShuttle() {
            @Override
            public @Nullable SqlNode visit(SqlIdentifier id) {
                return sqlIdentifierResolver.resolve(request.getEnviron(), id)
                                .map(translatedIdRef -> new SqlIdentifier(translatedIdRef.url(), id.getCollation(), id.getParserPosition()))
                                        .orElseGet(() -> (SqlIdentifier) super.visit(id));
            }
        });
        var sqlString = sqlNode.toSqlString(DuckDbDialect.INSTANCE);
        logger.infov("Received request for plan {0} and sql {1}", request.getPlan(), sqlString);
        var iterableResult = duckDbService.execSql(sqlString.getSql());
        var batchBuilder = Batch.newBuilder();
        iterableResult.forEach(strings -> {
            var rowBuilder = Row.newBuilder();
            for (String string : strings) {
                rowBuilder.addFields(ByteString.copyFrom(string, StandardCharsets.UTF_8));
            }
            batchBuilder.addRows(rowBuilder);
        });
        return Uni.createFrom().item(batchBuilder.build());
    }
}
