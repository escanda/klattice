package klattice.delphos;

import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
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

    @Inject
    SchemaRegistryStoreSource schemaRegistryStoreSource;

    public void answer(String query) throws SyntacticalException, PlanningException {
        Environment environ = environ();
        var qd = QueryDescriptor.newBuilder().setQuery(query).setEnviron(environ).build();
        var inflatedQuery = this.query.inflate(qd)
                .await()
                .indefinitely();
        if (inflatedQuery.getHasError()) {
            throw SyntacticalException.from(inflatedQuery.getDiagnostics());
        } else {
            var serializedPlan = inflatedQuery.getPlan();
            var replannedPlan = replan(environ, serializedPlan);
            var finalPlan = replannedPlan.getPlan();
        }
    }

    public ExpandedPlan replan(Environment environ, Plan plan) throws PlanningException {
        var expandedPlan = planner.expand(plan)
                .await()
                .indefinitely();
        if (expandedPlan.getHasError()) {
            throw PlanningException.from(expandedPlan.getDiagnostics());
        } else {
            return expandedPlan;
        }
    }

    private Environment environ() {
        var environBuilder = Environment.newBuilder();
        for (SchemaMetadata schemaMetadata : schemaRegistryStoreSource.allSchemas()) {
            environBuilder.addSchemas(Schema.newBuilder().setSchemaId(schemaMetadata.id()).build());
        }
        return environBuilder.build();
    }
}
