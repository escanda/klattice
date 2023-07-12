package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.substrait.proto.Plan;
import klattice.msg.QueryDescriptor;
import klattice.msg.SchemaDescriptor;
import klattice.query.Query;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.smallrye.common.constraint.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusIntegrationTest
public class PlannerServiceGrpcTest {
    @LoggerName("PlannerServiceGrpcTest")
    Logger logger;

    @GrpcClient
    Query query;

    @GrpcClient
    Planner planner;

    @Test
    public void smokeTest() {
        var preparedQueryUniAwait = query.prepare(QueryDescriptor.newBuilder().setQuery("SELECT * FROM PUBLIC.PUBLIC").addSources(SchemaDescriptor.newBuilder()
                .build()).build()).await();
        var preparedQuery = preparedQueryUniAwait.indefinitely();
        assertNotNull(preparedQuery);
        logger.infov("Prepared query plan is:\n{0}", new Object[]{preparedQuery});
        var enhancedPlan = planner.enhance(preparedQuery.getPlan());
        var p2 = enhancedPlan.await().indefinitely();
        assertNotNull(p2);
        assertEquals(1, p2.getRelationsCount());
        logger.infov("Planner became enhanced thus is:\n{0}", new Object[]{enhancedPlan});
    }
}
