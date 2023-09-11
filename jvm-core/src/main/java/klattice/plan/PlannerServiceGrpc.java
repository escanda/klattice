package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import klattice.msg.ExpandedPlan;
import klattice.msg.Plan;
import klattice.msg.PlanDiagnostics;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.jboss.logging.Logger;

import java.io.IOException;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Override
    public Uni<klattice.msg.ExpandedPlan> expand(klattice.msg.Plan request) {
        var environ = request.getEnviron();
        try {
            var reqPlan = request.getPlan();
            var relPlan = new ProtoPlanConverter().from(reqPlan);
            var relRoots = SubstraitToCalciteConverter.getRelRoots(relPlan);
            if (relRoots.isEmpty()) {
                var errorMessage = "Empty plans";
                logger.warn(errorMessage);
                return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(errorMessage).build()).build());
            } else {
                logger.infov("Original plan was:\n'{0}'\nNew plans are:\n'{1}'", new Object[]{request.getPlan(), relRoots});
                var relPlanBuilder = io.substrait.plan.ImmutablePlan.builder();
                var lst = relRoots.stream().map(relNode -> SubstraitRelVisitor.convert(RelRoot.of(relNode, SqlKind.SELECT), EXTENSION_COLLECTION, ImmutableFeatureBoard.builder().build())).toList();
                relPlanBuilder.roots(lst.stream().map(rel -> ImmutableRoot.builder().input(rel).build()).toList());
                var planProto = new PlanProtoConverter().toProto(relPlanBuilder.build());
                return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(false).setPlan(Plan.newBuilder().setEnviron(environ).setPlan(planProto)).build());
            }
        } catch (IOException e) {
            logger.error("Cannot enhance plan", e);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(e.getMessage())).build());
        }
    }
}
