package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import klattice.msg.QueryDescriptor;
import klattice.msg.SchemaDescriptor;
import klattice.query.Prepare;
import klattice.query.Query;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

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
    public void smokeTest() throws SqlParseException {
        var preparedQuery = prepare.compile("SELECT * FROM PUBLIC.PUBLIC", List.of(SchemaDescriptor.newBuilder().build()));
        assertNotNull(preparedQuery);
        logger.infov("Prepared query plan is:\n{0}", new Object[]{preparedQuery});
        var enhancedPlan = enhance.improve(preparedQuery.getPlan());
        assertNotNull(enhancedPlan);
        assertEquals(1, enhancedPlan.getRelationsCount());
        logger.infov("Planner became enhanced thus is now:\n{0}", new Object[]{enhancedPlan});
    }
}
