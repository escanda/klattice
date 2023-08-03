package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.msg.ExpandedPlan;
import klattice.msg.Plan;
import klattice.msg.PlanDiagnostics;
import org.jboss.logging.Logger;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Inject
    Expand expand;

    @Inject
    Unifier unifier;

    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Blocking
    @Override
    public Uni<klattice.msg.ExpandedPlan> expand(klattice.msg.Plan request) {
        var environ = request.getEnviron();
        try {
            var expanded = expand.expand(request.getPlan(), environ);
            var plans = requireNonNull(expanded.plans());
            var unified = unifier.unify(plans);
            var planBuilder = unified.planBuilder();
            if (planBuilder.isEmpty()) {
                logger.warnv("Error during plan unification {0}", new Object[]{unified.errorMessage()});
                return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(String.format(unified.errorMessage())).build()).build());
            } else {
                logger.infov("Original plan was:\n'{0}'\nNew plan is:\n'{1}'", new Object[]{request, expanded.actualPlan()});
                return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(false).setPlan(Plan.newBuilder().setEnviron(environ).setPlan(expanded.actualPlan())).build());
            }
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(e.getMessage())).build());
        }
    }
}
