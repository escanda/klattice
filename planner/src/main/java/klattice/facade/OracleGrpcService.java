package klattice.facade;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import klattice.grpc.ExecService;
import klattice.grpc.OracleService;
import klattice.grpc.PlannerService;
import klattice.grpc.QueryService;
import klattice.msg.*;
import klattice.store.SchemaMetadata;
import klattice.store.SchemaRegistryStoreSource;

@GrpcService
public class OracleGrpcService implements OracleService {
    @GrpcClient
    QueryService query;

    @GrpcClient
    PlannerService planner;

    @GrpcClient
    ExecService exec;

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
        return inflatedQuery.flatMap(preparedQuery -> {
            var serializedPlan = preparedQuery.getPlan();
            return replan(environ, serializedPlan).flatMap(expandedPlan -> {
                var finalPlan = expandedPlan.getPlan();
                return execute(finalPlan);
            });
        });
    }
}
