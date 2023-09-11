package klattice.delphos;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.exec.MutinyExecGrpc;
import klattice.msg.*;
import klattice.plan.MutinyPlannerGrpc;
import klattice.query.MutinyQueryGrpc;
import klattice.store.SchemaMetadata;
import klattice.store.SchemaRegistryStoreSource;

@Dependent
public class Oracle {
    @GrpcClient
    MutinyQueryGrpc.MutinyQueryStub query;

    @GrpcClient
    MutinyPlannerGrpc.MutinyPlannerStub planner;

    @GrpcClient
    MutinyExecGrpc.MutinyExecStub exec;

    @Inject
    SchemaRegistryStoreSource schemaRegistryStoreSource;

    public Uni<Batch> answer(String query) {
        Environment environ = environ();
        var qd = QueryDescriptor.newBuilder().setQuery(query).setEnviron(environ).build();
        var inflatedQuery = this.query.inflate(qd);
        return inflatedQuery.flatMap(Unchecked.function(preparedQuery -> {
            if (preparedQuery.getHasError()) {
                throw SyntacticalException.from(preparedQuery.getDiagnostics());
            } else {
                var serializedPlan = preparedQuery.getPlan();
                return replan(environ, serializedPlan).flatMap(Unchecked.function(expandedPlan -> {
                    var finalPlan = expandedPlan.getPlan();
                    return execute(finalPlan);
                }));
            }
        }));
    }

    public Uni<Batch> execute(Plan plan) {
        return exec.execute(plan);
    }

    public Uni<ExpandedPlan> replan(Environment environ, Plan plan) throws PlanningException {
        return planner.expand(plan);
    }

    public Environment environ() {
        var environBuilder = Environment.newBuilder();
        for (SchemaMetadata schemaMetadata : schemaRegistryStoreSource.allSchemas()) {
            environBuilder.addSchemas(Schema.newBuilder().setSchemaId(schemaMetadata.id()).build());
        }
        return environBuilder.build();
    }
}
