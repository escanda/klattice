package klattice.plan;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import klattice.msg.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class PlannerServiceGrpcTest {
    @GrpcClient
    Planner planner;

    @Test
    public void smokeTest() {
        var relDescriptor = Rel.newBuilder()
                .setRelName("public_table")
                .addAllColumns(
                        List.of(
                                Column.newBuilder().setColumnName("public").setType(Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build())
                        .build())
                .build()))
            .build();
        var nonExpanded = Plan.newBuilder().setEnviron(Environment.newBuilder().addSchemas(Schema.newBuilder().setSchemaId(1).setRelName("public").addRels(relDescriptor).build()).build()).build();
        var expanded = planner.expand(nonExpanded);
        assertNotNull(expanded);
        var expandedPlan = expanded.await().atMost(Duration.ofMinutes(1));
        assertNotNull(expandedPlan);
        assertTrue(expandedPlan.hasPlan());
    }
}
