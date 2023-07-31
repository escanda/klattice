package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.msg.ExpandedPlan;
import klattice.msg.Plan;
import klattice.msg.PlanDiagnostics;
import org.apache.calcite.util.Pair;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Collection;

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
    public Uni<klattice.msg.ExpandedPlan> expand(klattice.msg.Plan request) {
        var environList = request.getEnvironList();
        Pair<Collection<io.substrait.proto.Plan>, io.substrait.proto.Plan> pair;
        try {
            pair = expand.expand(request.getPlan(), environList);
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setErrored(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(String.format("Error expanding plan %s", e.getMessage()))).build());
        }
        var plans = requireNonNull(pair.getKey());
        var unificated = unify.unification(plans);
        var planBuilder = unificated.planBuilder;
        if (planBuilder.isEmpty()) {
            logger.warnv("Error during plan unification {0}", new Object[]{unificated.errorMessage});
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setErrored(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(String.format("Error during plan unification %s", unificated.errorMessage)).build()).build());
        } else {
            logger.infov("Original plan was:\n{0}\nNew plan is:\n{1}", new Object[]{request, pair.getValue()});
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setErrored(false).setPlan(Plan.newBuilder().setPlan(planBuilder.get()).addAllEnviron(request.getEnvironList())).build()); // TODO: add discovered environs
        }
    }
}
