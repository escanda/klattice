package klattice.api;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class SchemaServiceGrpc implements Schema {
    @Override
    public Uni<Schemata> query(SchemaQuery request) {
        return Uni.createFrom().item(Schemata.newBuilder().build());
    }
}
