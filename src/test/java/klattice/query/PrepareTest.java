package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.*;
import klattice.plan.Enhance;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class PrepareTest {
    @LoggerName("QueryServiceGrpcTest")
    Logger logger;

    @Inject
    Prepare prepare;

    @Test
    public void smokeTest() throws SqlParseException {
        var type = Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build());
        var col = Column.newBuilder().setColumnName("public").setType(type.build());
        var projection = Rel.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col.build()))
                .build();
        var environments = List.of(Environment.newBuilder().setSchemaId(1).setRelName("PUBLIC").addRels(projection).build());
        var q = "SELECT 'public' FROM PUBLIC";
        var qc = QueryDescriptor.newBuilder()
                .setQuery(q)
                .addAllEnviron(environments)
                .build();
        var preparedQuery = prepare.compile(qc.getQuery(), environments);
        logger.info(preparedQuery);
        assertNotNull(preparedQuery);
        assertFalse(preparedQuery.getErrored());
    }
}
