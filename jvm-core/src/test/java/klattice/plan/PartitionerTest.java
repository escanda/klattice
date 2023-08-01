package klattice.plan;

import io.quarkus.test.junit.QuarkusTest;
import io.substrait.proto.Type;
import jakarta.inject.Inject;
import klattice.msg.*;
import klattice.schema.SchemaFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;

import java.util.List;

@QuarkusTest
public class PartitionerTest {
    @Inject
    Partitioner partitioner;

    @Test
    public void smokeTest() throws Exception {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var environ = Environment.newBuilder()
                .setRelName("PUBLIC")
                .addRels(Rel.newBuilder()
                        .setRelName("PUBLIC")
                        .addColumns(Column.newBuilder()
                                .setColumnName("PUBLIC")
                                .setType(Type.newBuilder()
                                        .setBool(Type.Boolean.newBuilder())))
                        .setNextEndpoint(Endpoint.newBuilder()
                                .setResponseAdaptorValue(ResponseAdaptorKind.ARROW_VALUE)
                                .setHostPort(HostAndPort.newBuilder()
                                        .setHost("localhost")
                                        .setPort(9696))));
        var query = "SELECT count(PUBLIC) FROM PUBLIC.PUBLIC";
        var inspector = new SchemaFactory(List.of(environ.build()));
        var rootSchema = inspector.getCatalog().getRootSchema();
        var planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.plus())
                .build());
        var sqlNode = planner.parse(query);
        var rewrittenSqlNode = planner.validate(sqlNode);
        var relNode = planner.rel(rewrittenSqlNode);
        var partialPlans = partitioner.differentiate(typeFactory, rootSchema, relNode.rel);
        System.out.println(partialPlans);
    }
}
