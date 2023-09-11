package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class PrepareTest {
    @LoggerName("PrepareTest")
    Logger logger;

    @Inject
    Prepare prepare;
    @Test
    public void smokeTest() throws SqlParseException, RelConversionException, ValidationException {
        var type = Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build());
        var col = Column.newBuilder().setColumnName("public").setType(type.build());
        var projection = Rel.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addAllColumns(List.of(col.build()))
                .build();
        var environ = Environment.newBuilder().addSchemas(Schema.newBuilder().setSchemaId(1).setRelName("PUBLIC").addRels(projection)).build();
        var q = "SELECT 'public' FROM PUBLIC.PUBLIC";
        var preparedQuery = prepare.compile(q, environ);
        logger.info(preparedQuery);
        assertNotNull(preparedQuery);
        assertFalse(preparedQuery.getHasError());
    }
}
