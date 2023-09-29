package klattice.exec;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.grpc.ExecService;
import klattice.msg.Batch;
import klattice.msg.Plan;
import org.jboss.logging.Logger;

@GrpcService
public class ExecGrpcService implements ExecService {
    @LoggerName("ExecGrpcService")
    Logger logger;

    @Inject
    Exec exec;

    @Blocking
    @Override
    public Uni<Batch> execute(Plan request) {
        var sql = exec.toSql(request.getEnviron(), request.getPlan());
        logger.infov("Received request with sql {0}", sql);
        var batch = exec.execute(sql);
        return Uni.createFrom().item(batch);
    }
}
