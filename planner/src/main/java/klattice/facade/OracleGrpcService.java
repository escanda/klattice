package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.inject.Inject;
import klattice.exec.Exec;
import klattice.msg.*;
import klattice.plan.Planner;
import klattice.query.Query;
import klattice.store.SchemaMetadata;
import klattice.store.SchemaRegistryStoreSource;

@GrpcService
public class OracleGrpcService implements Oracle {
    @GrpcClient
    Query query;

    @GrpcClient
    Planner planner;

    @GrpcClient
    Exec exec;

    @Inject
    SchemaRegistryStoreSource schemaRegistryStoreSource;

    protected Uni<Batch> execute(Plan plan) {
        return exec.execute(plan);
    }

    protected Uni<ExpandedPlan> replan(Environment environ, Plan plan) {
        return planner.expand(plan);
    }

    protected Environment environ() {
        var environBuilder = Environment.newBuilder();
        for (SchemaMetadata schemaMetadata : schemaRegistryStoreSource.allSchemas()) {
            environBuilder.addSchemas(Schema.newBuilder().setSchemaId(schemaMetadata.id()).build());
        }
        return environBuilder.build();
    }

    @Blocking
    @Override
    public Uni<Batch> answer(klattice.msg.Query request) {
        Environment environ = environ();
        var qd = QueryDescriptor.newBuilder().setQuery(request.getQuery()).setEnviron(environ).build();
        var inflatedQuery = this.query.inflate(qd);
        return inflatedQuery.flatMap(Unchecked.function(preparedQuery -> {
            var serializedPlan = preparedQuery.getPlan();
            return replan(environ, serializedPlan).flatMap(Unchecked.function(expandedPlan -> {
                var finalPlan = expandedPlan.getPlan();
                return execute(finalPlan);
            }));
        }));
    }
}
