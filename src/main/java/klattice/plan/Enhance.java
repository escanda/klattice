package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.proto.Plan;
import io.substrait.relation.ProtoRelConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.dialect.DucksDbDialect;
import klattice.msg.SchemaDescriptor;
import klattice.schema.SchemaDescriptorFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Dependent
public class Enhance {
    @LoggerName("Enhance")
    Logger logger;

    @Inject
    Partitioner partitioner;

    public Plan improve(Plan source, List<SchemaDescriptor> sources) throws IOException {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        optimizePlan(plan, sources);
        return plan.build();
    }

    protected void optimizePlan(Plan.Builder plan, Collection<SchemaDescriptor> sources) throws IOException {
        var extensionCollector = new ExtensionCollector();
        var sourceList = new ArrayList<>(sources);
        var factory = new SchemaDescriptorFactory(sourceList);
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
        }).filter(Optional::isPresent).map(opt -> (RelNode) opt.get()).map(relNode -> {
            var fields = relNode.getRowType().getFieldList().stream().map(relDataTypeField -> Pair.of(relDataTypeField.getIndex(), relDataTypeField.getName()));
            var fieldList = fields.toList();
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var catalog = factory.getCatalog();
            var relOptCluster = factory.getRelOptCluster();
            var sqlValidator = factory.getSqlValidator();
            var converter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());
            var relToSqlConverter = new RelToSqlConverter(DucksDbDialect.INSTANCE);
            var relConverter = new SqlToRelConverter(null, sqlValidator, catalog, relOptCluster, call -> SqlRexContext::convertExpression, SqlToRelConverter.CONFIG);
            var project = RelOptUtil.createProject(relNode, new Mappings.IdentityMapping(fieldList.size()));
            var relShuttle = new RelShuttleImpl();
            var toVisit = project.accept(relShuttle);
            var q = relToSqlConverter.visitInput(toVisit, 0).asQueryOrValues();
            var rexNode = converter.convertExpression(q);
            var finalRel = partitioner.partition(catalog.getRootSchema(), relConverter, rexNode, q);
            var visitedResult = relToSqlConverter.visit((Project) finalRel);
            var select = visitedResult.asSelect();
            var newRexNode = converter.convertExpression(select);
            newRexNode.accept(new RexVisitor<Void>() {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    return null;
                }

                @Override
                public Void visitLocalRef(RexLocalRef localRef) {
                    return null;
                }

                @Override
                public Void visitLiteral(RexLiteral literal) {
                    return null;
                }

                @Override
                public Void visitCall(RexCall call) {
                    return null;
                }

                @Override
                public Void visitOver(RexOver over) {
                    return null;
                }

                @Override
                public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                    return null;
                }

                @Override
                public Void visitDynamicParam(RexDynamicParam dynamicParam) {
                    return null;
                }

                @Override
                public Void visitRangeRef(RexRangeRef rangeRef) {
                    return null;
                }

                @Override
                public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                    return null;
                }

                @Override
                public Void visitSubQuery(RexSubQuery subQuery) {
                    return null;
                }

                @Override
                public Void visitTableInputRef(RexTableInputRef fieldRef) {
                    return null;
                }

                @Override
                public Void visitPatternFieldRef(RexPatternFieldRef fieldRef) {
                    return null;
                }
            });
            var relRoot = new RelRoot(relNode, relNode.getRowType(), SqlKind.SELECT, fieldList, RelCollations.EMPTY, List.of());
            return Converter.getPlan(relRoot).build();
        }).toList();
        plan.clearRelations();
        plan.addAllRelations(plans.stream().flatMap(plan1 -> plan1.getRelationsList().stream()).toList());
    }
}
