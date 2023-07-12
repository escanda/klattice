package klattice.api.plan;

import com.google.protobuf.Syntax;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.api.Projection;
import klattice.api.QueryContext;
import klattice.api.SchemaSourceDetails;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
public class QueryServiceGrpcTest {
    @Inject
    Enhancer enhancer;

    @Inject
    Parser parser;

    @Test
    public void smokeTest() throws SqlParseException {
        var projection = Projection.newBuilder()
                .setSchemaId(1)
                .setRelName("PUBLIC")
                .addTyping(Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build()))
                .build();
        var schemaSources = List.of(SchemaSourceDetails.newBuilder().setSchemaId(1).setRelName("PUBLIC").addProjections(projection).build());

        var q = "SELECT * FROM PUBLIC.PUBLIC";
        var sqlNode = parser.parse(QueryContext.newBuilder()
                .setQuery(q)
                        .addAllSources(schemaSources)
                .build());
        assertNotNull(sqlNode);
        var inflated = enhancer.inflate(sqlNode, schemaSources);
        System.out.println(inflated);
    }
}
