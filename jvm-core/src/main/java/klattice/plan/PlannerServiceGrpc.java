package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import klattice.msg.ExpandedPlan;
import klattice.msg.Plan;
import klattice.msg.PlanDiagnostics;
import klattice.substrait.Shared;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.jboss.logging.Logger;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @LoggerName("PlannerServiceGrpc")
    Logger logger;

    @Blocking
    @Override
    public Uni<klattice.msg.ExpandedPlan> expand(klattice.msg.Plan request) {
        var environ = request.getEnviron();
        var reqPlan = request.getPlan();
        var relPlan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(reqPlan);
        var relRoots = SubstraitToCalciteConverter.getRelRoots(relPlan);
        if (relRoots.isEmpty()) {
            var errorMessage = "Empty plans";
            logger.warn(errorMessage);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(true).setDiagnostics(PlanDiagnostics.newBuilder().setErrorMessage(errorMessage).build()).build());
        } else {
            var replannedRelNode = relRoots.stream().findFirst().orElse(null).accept(new Replanner(environ));
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var relPlanBuilder = io.substrait.plan.ImmutablePlan.builder();
            var rel = Shared.createSubstraitRelVisitor(typeFactory, Shared.additionalSignatures).apply(replannedRelNode);
            relPlanBuilder.addRoots(ImmutableRoot.builder().input(rel).build());
            var planProto = new PlanProtoConverter().toProto(relPlanBuilder.build());
            logger.infov("Original plan was:\n {0} \nNew plan is:\n {1}", request.getPlan(), planProto);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(false).setPlan(Plan.newBuilder().setEnviron(environ).setPlan(planProto)).build());
        }
    }
}
