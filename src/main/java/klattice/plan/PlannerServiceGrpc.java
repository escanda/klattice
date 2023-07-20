package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import klattice.msg.PlanDescriptor;
import org.jboss.logging.Logger;

import java.io.IOException;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Inject
    Enhance enhance;

    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Override
    public Uni<PlanDescriptor> enhance(PlanDescriptor request) {
        Plan improvedPlan;
        try {
            improvedPlan = enhance.improve(request.getPlan(), request.getSourcesList());
            logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, improvedPlan});
            return Uni.createFrom().item(PlanDescriptor.newBuilder().addAllSources(request.getSourcesList()).setPlan(improvedPlan).build());
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(() -> null);
        }
    }
}