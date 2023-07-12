package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Inject
    Enhance enhance;

    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Override
    public Uni<Plan> enhance(Plan request) {
        var improvedPlan = enhance.improve(request);
        logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, improvedPlan});
        return Uni.createFrom().item(improvedPlan);
    }
}
