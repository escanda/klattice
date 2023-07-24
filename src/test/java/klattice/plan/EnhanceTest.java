package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.ColumnDescriptor;
import klattice.msg.QueryDescriptor;
import klattice.msg.RelDescriptor;
import klattice.msg.SchemaDescriptor;
import klattice.query.Prepare;
import klattice.query.Query;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.smallrye.common.constraint.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class EnhanceTest {
    @LoggerName("PlannerServiceGrpcTest")
    Logger logger;

    @Inject
    Prepare prepare;

    @Inject
    Enhance enhance;

    @Test
    public void smokeTest() throws SqlParseException, IOException {
        var type = Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build());
        var col = ColumnDescriptor.newBuilder().setColumnName("public").setType(type.build()).build();
        var projection = RelDescriptor.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col))
                .build();
        var schemaSources = List.of(SchemaDescriptor.newBuilder().setSchemaId(1).setRelName("PUBLIC").addProjections(projection).build());

        var preparedQuery = prepare.compile("SELECT 'public' FROM PUBLIC", schemaSources);
        assertNotNull(preparedQuery);
        logger.infov("Prepared query plan is:\n{0}", new Object[]{preparedQuery});
        var plan = preparedQuery.getPlan().getPlan();
        var enhancedPlan = enhance.improve(plan, schemaSources);
        assertNotNull(enhancedPlan);
        assertEquals(1, enhancedPlan.getRelationsCount());
        logger.infov("Planner became enhanced thus is now:\n{0}", new Object[]{enhancedPlan});
    }
}
