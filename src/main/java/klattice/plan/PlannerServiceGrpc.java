package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Inject
    Enhance enhance;

    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Override
    public Uni<klattice.msg.Plan> enhance(klattice.msg.Plan request) {
        Plan improvedPlan;
        try {
            var environList = request.getEnvironList();
            improvedPlan = enhance.improve(request.getPlan(), environList);
            logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, improvedPlan});
            return Uni.createFrom().item(klattice.msg.Plan.newBuilder().addAllEnviron(environList).setPlan(improvedPlan).build());
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(() -> null);
        }
    }
}
