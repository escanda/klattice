package klattice.plan;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Plan;
import klattice.msg.Rel;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusIntegrationTest
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
        var nonExpanded = Plan.newBuilder().addEnviron(Environment.newBuilder().setSchemaId(1).setRelName("public").addRels(relDescriptor).build()).build();
        var expanded = planner.expand(nonExpanded);
        assertNotNull(expanded);
        System.err.println(expanded);
    }
}
