package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.ColumnDescriptor;
import klattice.msg.QueryDescriptor;
import klattice.msg.RelDescriptor;
import klattice.msg.SchemaDescriptor;
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
        var col = ColumnDescriptor.newBuilder().setColumnName("public").setType(type.build());
        var projection = RelDescriptor.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col.build()))
                .build();
        var schemaSources = List.of(SchemaDescriptor.newBuilder().setSchemaId(1).setRelName("PUBLIC").addProjections(projection).build());

        var q = "SELECT 'public' FROM PUBLIC";
        var qc = QueryDescriptor.newBuilder()
                .setQuery(q)
                .addAllSources(schemaSources)
                .build();
        var preparedQuery = prepare.compile(qc.getQuery(), qc.getSourcesList());
        logger.info(preparedQuery);
        assertNotNull(preparedQuery);
        assertFalse(preparedQuery.getErrored());
    }
}
