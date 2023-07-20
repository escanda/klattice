package klattice.plan;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import klattice.msg.PlanDescriptor;
import klattice.msg.RelDescriptor;
import klattice.msg.SchemaDescriptor;
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
        var relDescriptor = RelDescriptor.newBuilder().setRelName("public_table").addAllColumnName(List.of("public_field")).build();
        var enhancedPlan = planner.enhance(PlanDescriptor.newBuilder()
                .addSources(SchemaDescriptor.newBuilder()
                        .setSchemaId(1)
                        .setRelName("public")
                        .addAllProjections(List.of(relDescriptor)).build()).build());
        assertNotNull(enhancedPlan);
        System.err.println(enhancedPlan);
    }
}
