package klattice.plan;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.proto.Plan;

@GrpcService
public class PlannerServiceGrpc implements Planner {
    @Override
    public Uni<Plan> enhance(Plan request) {
        return Uni.createFrom().item(request);
    }
}
