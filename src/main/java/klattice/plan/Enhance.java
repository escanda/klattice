package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.proto.Plan;
import io.substrait.relation.ProtoRelConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.dialect.DucksDbDialect;
import klattice.msg.Environment;
import klattice.schema.SchemaDescriptorFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Dependent
public class Enhance {
    @LoggerName("Enhance")
    Logger logger;

    @Inject
    Partitioner partitioner;

    @GrpcClient
    Planner planner;

    public Plan improve(Plan source, List<Environment> environments) throws IOException {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        optimizePlan(plan, environments);
        return plan.build();
    }

    protected void optimizePlan(Plan.Builder plan, List<Environment> environments) throws IOException {
        var extensionCollector = new ExtensionCollector();
        var environmentList = new ArrayList<>(environments);
        var factory = new SchemaDescriptorFactory(environmentList);
        final ProtoRelConverter protoRelConverter = new ProtoRelConverter(extensionCollector);
        List<Plan> plans = plan.getRelationsList().stream().map(planRel -> protoRelConverter.from(planRel.getRoot().getInput())).flatMap(rel -> {
            if (rel == null) return Stream.empty();
            var config = Frameworks.newConfigBuilder()
                    .defaultSchema(factory.getCatalog().getRootSchema().plus())
                    .build();
            var relNodeConverter = new SubstraitRelNodeConverter(Converter.EXTENSION_COLLECTION, factory.getTypeFactory(), RelBuilder.create(config));
            var relNode = rel.accept(relNodeConverter);
            return Stream.of(relNode);
        }).flatMap(relNode -> {
            var catalog = factory.getCatalog();
            var relOptCluster = factory.getRelOptCluster();
            var sqlValidator = factory.getSqlValidator();
            var converter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());
            var relToSqlConverter = new JdbcImplementor(DucksDbDialect.INSTANCE, factory.getTypeFactory());
            var relConverter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, call -> SqlRexContext::convertExpression, SqlToRelConverter.CONFIG);
            var relShuttle = new RelShuttleImpl();
            var toVisit = relNode.accept(relShuttle);
            var sqlNode = relToSqlConverter.implement(toVisit);
            var rexNode = converter.convertExpression(sqlNode.asQueryOrValues());
            var schema = Converter.getSchema(environments);
            var partitions = List.of(toVisit);
            return partitions.stream().map(relNode1 -> {
                var relFieldList = relNode1.getRowType().getFieldList().stream().map(relDataTypeField -> Pair.of(relDataTypeField.getIndex(), relDataTypeField.getName())).toList();
                var relRoot = new RelRoot(relNode1, relNode1.getRowType(), SqlKind.SELECT, relFieldList, RelCollations.EMPTY, List.of());
                return Converter.getPlan(relRoot).build();
            });
        }).toList();
        plan.clearRelations();
        plan.addAllRelations(plans.stream().flatMap(plan1 -> plan1.getRelationsList().stream()).toList());
    }
}
