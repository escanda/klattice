package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import klattice.calcite.DucksDbDialect;
import klattice.msg.ExpandedPlan;
import klattice.msg.Plan;
import klattice.msg.PlanDiagnostics;
import klattice.schema.SchemaFactory;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;
import org.jboss.logging.Logger;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;
import static klattice.substrait.Shared.*;

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
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var schemaFactory = new SchemaFactory(new SqlStdOperatorTable(), environ);
            var planner = schemaFactory.getRelOptCluster().getPlanner();
            var optimizedRelNodes = relRoots.stream().map(relNode -> {
                planner.setRoot(relNode);
                return planner.findBestExp();
            }).toList();
            var framework = framework(schemaFactory);
            var relBuilder = RelBuilder.create(framework);
            var replannedRelNodes = optimizedRelNodes.stream()
                    .map(relNode -> relNode.accept(new Replanner(environ, typeFactory, relBuilder)))
                    .toList();
            var relToSqlConverter = new RelToSqlConverter(DucksDbDialect.INSTANCE);
            var rewrittenNodes = replannedRelNodes.stream()
                    .map(relToSqlConverter::visitRoot)
                    .map(SqlImplementor.Result::asSelect)
                    .map(sqlSelect -> (SqlSelect) sqlSelect.accept(new Renamer()))
                    .map(sqlSelect -> createSqlToRelConverter(schemaFactory).convertSelect(sqlSelect, true))
                    .toList();
            var relPlanBuilder = io.substrait.plan.ImmutablePlan.builder();
            var substraitRelVisitor = createSubstraitRelVisitor(typeFactory);
            var rels = rewrittenNodes.stream().map(substraitRelVisitor::apply).toList();
            relPlanBuilder.roots(rels.stream().map(rel -> ImmutableRoot.builder().input(rel).build()).toList());
            var planProto = new PlanProtoConverter().toProto(relPlanBuilder.build());
            logger.infov("Original plan was:\n {0} \nNew plan is:\n {1}", request.getPlan(), planProto);
            return Uni.createFrom().item(ExpandedPlan.newBuilder().setHasError(false).setPlan(Plan.newBuilder().setEnviron(environ).setPlan(planProto)).build());
        }
    }

}
