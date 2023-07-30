package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Collection;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Inject
    Expand expand;

    @Inject
    Unify unify;

    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Override
    public Uni<klattice.msg.Plan> expand(klattice.msg.Plan request) {
        try {
            var environList = request.getEnvironList();
            var pair  = expand.expand(request.getPlan(), environList);
            var plan = unify.unification(pair.getKey());
            logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, pair.getValue()});
            return Uni.createFrom().item(klattice.msg.Plan.newBuilder().addAllEnviron(environList).setPlan(pair.getValue()).build());
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(() -> null);
        }
    }

    private Plan unifyPlans(Plan parentPlan, Collection<Plan> plans) {
        // TODO: merge all plans into a single rel with filters
        return Plan.newBuilder().mergeFrom(parentPlan).build();
    }
}
