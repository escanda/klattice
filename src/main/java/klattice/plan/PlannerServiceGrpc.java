package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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
            var plans = requireNonNull(pair.getKey());
            var plan = unify.unification(plans);
            logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, pair.getValue()});
            return Uni.createFrom().item(klattice.msg.Plan.newBuilder().addAllEnviron(environList).setPlan(plan).build());
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(() -> request);
        }
    }
}
