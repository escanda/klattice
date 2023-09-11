package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.exec.Exec;
import klattice.msg.*;
import klattice.plan.Planner;
import klattice.query.Query;
import klattice.store.SchemaMetadata;
import klattice.store.SchemaRegistryStoreSource;

@ApplicationScoped
public class Oracle {
    @GrpcClient
    Query query;

    @GrpcClient
    Planner planner;

    @GrpcClient
    Exec exec;

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
