package klattice.plan;

import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class PlannerServiceGrpcTest {
    @GrpcClient
    Planner planner;

    @Test
    public void smokeTest() throws InvalidProtocolBufferException {
        var relDescriptor = Rel.newBuilder()
                .setRelName("public_table")
                .addAllColumns(
                        List.of(
                                Column.newBuilder().setColumnName("public").setType(Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build())
                        .build())
                .build()))
            .build();
        var actualPlan = io.substrait.proto.Plan.parseFrom((ByteBuffer) null); // TODO: insert payload
        var nonExpanded = Plan.newBuilder().setPlan(actualPlan).setEnviron(Environment.newBuilder().addSchemas(Schema.newBuilder().setSchemaId(1).setRelName("public").addRels(relDescriptor).build()).build()).build();
        var expanded = planner.expand(nonExpanded);
        assertNotNull(expanded);
        var expandedPlan = expanded.await().atMost(Duration.ofMinutes(1));
        assertNotNull(expandedPlan);
        assertTrue(expandedPlan.hasPlan());
    }
}
