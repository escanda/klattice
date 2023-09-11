package klattice.exec;

import com.google.protobuf.ByteString;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Plan;
import klattice.msg.Row;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;

@GrpcService
public class ExecGrpcService implements Exec {
    @LoggerName("ExecGrpcService")
    Logger logger;

    @Inject
    DuckDbService duckDbService;

    @Blocking
    @Override
    public Uni<Batch> execute(Plan request) {
        var substraitPlanBytes = request.getPlan().toByteArray();
        logger.infov("Received request {0}", substraitPlanBytes);
        var iterableResult = duckDbService.execSubstrait(substraitPlanBytes);
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
