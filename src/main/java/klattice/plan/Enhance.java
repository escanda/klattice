package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcClient;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.proto.Plan;
import io.substrait.relation.ProtoRelConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.dialect.DucksDbDialect;
import klattice.msg.Environment;
import klattice.schema.SchemaDescriptorFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
        List<Plan> plans = plan.getRelationsList().stream().map(planRel -> {
            try {
                final ProtoRelConverter protoRelConverter = new ProtoRelConverter(extensionCollector);
                return protoRelConverter.from(planRel.getRoot().getInput());
            } catch (IOException e) {
                logger.warn("Cannot convert relationship {0} to proto", e);
                return null;
            }
        }).map(rel -> {
            if (rel == null) return Optional.empty();
            var substraitToCalcite = new SubstraitToCalcite(Converter.EXTENSION_COLLECTION, factory.getTypeFactory());
            var relNode = substraitToCalcite.convert(rel);
            return Optional.of(relNode);
        }).filter(Optional::isPresent).map(opt -> (RelNode) opt.get()).flatMap(relNode -> {
            var fields = relNode.getRowType().getFieldList().stream().map(relDataTypeField -> Pair.of(relDataTypeField.getIndex(), relDataTypeField.getName()));
            var fieldList = fields.toList();
            var catalog = factory.getCatalog();
            var relOptCluster = factory.getRelOptCluster();
            var sqlValidator = factory.getSqlValidator();
            var converter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());
            var relToSqlConverter = new RelToSqlConverter(DucksDbDialect.INSTANCE);
            var relConverter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, call -> SqlRexContext::convertExpression, SqlToRelConverter.CONFIG);
            var project = RelOptUtil.createProject(relNode, new Mappings.IdentityMapping(fieldList.size()));
            var relShuttle = new RelShuttleImpl();
            var toVisit = project.accept(relShuttle);
            var sqlNode = relToSqlConverter.visit((Project) toVisit).asQueryOrValues();
            var rexNode = converter.convertExpression(sqlNode);
            var schema = Converter.getSchema(environments);
            var partitions = partitioner.partition(schema, relConverter, rexNode, sqlNode);
            return partitions
                    .stream()
                    .map(relNode1 -> {
                        var relFieldList = relNode1.getRowType().getFieldList().stream()
                                .map(relDataTypeField -> Pair.of(relDataTypeField.getIndex(), relDataTypeField.getName()))
                                .toList();
                        var relRoot = new RelRoot(relNode1, relNode1.getRowType(), SqlKind.SELECT, relFieldList, RelCollations.EMPTY, List.of());
                        return Converter.getPlan(relRoot).build();
                    });
        }).toList();
        plan.clearRelations();
        plan.addAllRelations(plans.stream().flatMap(plan1 -> plan1.getRelationsList().stream()).toList());
    }
}
