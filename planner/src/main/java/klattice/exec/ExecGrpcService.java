package klattice.exec;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.grpc.ExecService;
import klattice.msg.Batch;
import klattice.msg.ExpandedPlan;
import org.jboss.logging.Logger;

@GrpcService
public class ExecGrpcService implements ExecService {
    @LoggerName("ExecGrpcService")
    Logger logger;

    @Inject
    Exec exec;

    @Blocking
    @Override
    public Uni<Batch> execute(ExpandedPlan request) {
        var sqlStatement = request.getSqlStatements().getSqlStatementList().stream().findFirst().orElseThrow();
        logger.infov("Received request with sql {0}", sqlStatement);
        var batch = exec.execute(sqlStatement);
        return Uni.createFrom().item(batch);
    }
}
