package klattice.plan;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import klattice.msg.ColumnDescriptor;
import klattice.msg.Environment;
import klattice.msg.PlanDescriptor;
import klattice.msg.RelDescriptor;
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
        var relDescriptor = RelDescriptor.newBuilder()
                .setRelName("public_table")
                .addAllColumns(
                        List.of(
                                ColumnDescriptor.newBuilder().setColumnName("public").setType(Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build())
                        .build())
                .build()))
            .build();
        var enhancedPlan = planner.enhance(PlanDescriptor.newBuilder().addEnviron(Environment.newBuilder().setSchemaId(1).setRelName("public").addAllRels(List.of(relDescriptor)).build()).build());
        assertNotNull(enhancedPlan);
        System.err.println(enhancedPlan);
    }
}
