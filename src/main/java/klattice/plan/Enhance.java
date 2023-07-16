package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.proto.Plan;
import io.substrait.relation.Extension;
import io.substrait.relation.ProtoRelConverter;
import jakarta.enterprise.context.Dependent;
import klattice.msg.SchemaDescriptor;
import klattice.schema.SchemaDescriptorInspector;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.LoptSemiJoinOptimizer;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;

@Dependent
public class Enhance {
    @LoggerName("Enhance")
    Logger logger;

    public Plan improve(Plan source, List<SchemaDescriptor> sources) throws IOException {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        optimizePlan(plan, sources);
        return plan.build();
    }

    protected void optimizePlan(Plan.Builder plan, Collection<SchemaDescriptor> sources) throws IOException {
        var extensionCollector = new ExtensionCollector();
        var relNodeOpt = plan.getRelationsList().stream().map(planRel -> {
            ProtoRelConverter protoRelConverter = null;
            try {
                protoRelConverter = new ProtoRelConverter(extensionCollector);
                return protoRelConverter.from(planRel.getRoot().getInput());
            } catch (IOException e) {
                logger.warn("Cannot convert relationship {0} to proto", e);
                return null;
            }
        }).filter(Objects::nonNull).findFirst().flatMap(rel -> {
            var inspector = new SchemaDescriptorInspector(new ArrayList<>(sources));
            var substraitToCalcite = new SubstraitToCalcite(ImmutableSimpleExtension.ExtensionCollection.builder().build(), inspector.getTypeFactory());
            var relNode = substraitToCalcite.convert(rel);
            return Optional.ofNullable(relNode);
        });
        plan.clearRelations();
        plan.addAllRelations(relNodeOpt.map(relNode -> {
            var fields = relNode.getRowType().getFieldList().stream().map(relDataTypeField -> Pair.of(relDataTypeField.getIndex(), relDataTypeField.getName()));
            var relRoot = new RelRoot(relNode, relNode.getRowType(), SqlKind.SELECT, fields.toList(), RelCollations.EMPTY, List.of());
            return Converter.getPlan(relRoot).getRelationsList();
        }).orElse(Collections.emptyList()));
    }
}
