package klattice.exec;

import com.google.protobuf.ByteString;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.plan.ProtoPlanConverter;
import jakarta.inject.Inject;
import klattice.calcite.DuckDbDialect;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Plan;
import klattice.msg.Row;
import klattice.substrait.SubstraitToCalciteConverter;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@GrpcService
public class ExecGrpcService implements Exec {
    @LoggerName("ExecGrpcService")
    Logger logger;

    @Inject
    DuckDbService duckDbService;

    @Blocking
    @Override
    public Uni<Batch> execute(Plan request) {
        var plan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(request.getPlan());
        var relRoots = SubstraitToCalciteConverter.getRelRoots(plan);
        var sqlStmts = relRoots.stream().map(relNode -> SubstraitToSql.toSql(relNode, DuckDbDialect.INSTANCE)).toList();
        var sql = sqlStmts.stream().findFirst().orElseThrow();
        logger.infov("Received request for plan {0} and sql {1}", request.getPlan(), sql);
        var iterableResult = duckDbService.execSql(sql);
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
