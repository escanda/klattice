package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Rel;
import klattice.query.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.smallrye.common.constraint.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class ExpandTest {
    @LoggerName("PlannerServiceGrpcTest")
    Logger logger;

    @Inject
    Prepare prepare;

    @Inject
    Expand expand;

    @Test
    public void smokeTest() throws SqlParseException, IOException, RelConversionException, ValidationException {
        var type = Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build());
        var col = Column.newBuilder().setColumnName("public").setType(type.build()).build();
        var projection = Rel.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col))
                .build();
        var schemaSources = List.of(Environment.newBuilder().setSchemaId(1).setRelName("PUBLIC").addRels(projection).build());
        var preparedQuery = prepare.compile("SELECT * FROM PUBLIC.PUBLIC", schemaSources);
        assertNotNull(preparedQuery);
        logger.infov("Prepared query plan is:\n{0}", new Object[]{preparedQuery});
        var shrunk = preparedQuery.getPlan().getPlan();
        var expanded = expand.expand(shrunk, schemaSources);
        assertNotNull(expanded);
        assertNotNull(expanded.actualPlan());
        assertEquals(1, expanded.plans().size());
        logger.infov("Planner became expand thus is now:\n{0}", new Object[]{expanded.actualPlan()});
    }
}
