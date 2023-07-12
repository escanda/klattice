package klattice.api;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class SchemaServiceGrpc implements Schema {
    @Override
    public Uni<SchemaSourceDetails> query(SchemaQuery request) {
        return Uni.createFrom().item(SchemaSourceDetails.newBuilder().build());
    }
}
