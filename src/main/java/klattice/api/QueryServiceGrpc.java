package klattice.api;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;
import org.jboss.logging.Logger;

import static io.quarkus.logging.Log.log;

@GrpcService
public class QueryServiceGrpc implements Query {
    @Override
    public Uni<Plan> prepare(QueryContext request) {
        var q = request.getQuery();
        var plan = Plan.newBuilder();
        log("grpc", Logger.Level.INFO, "Sending plan {} as prepare request for query: {}", new Object[]{ q, plan }, null);

        return Uni.createFrom().item(plan.build());
    }
}
