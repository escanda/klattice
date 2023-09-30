package klattice.plan.rule;

import jakarta.annotation.Nullable;
import klattice.calcite.BuiltinTables;
import klattice.calcite.FunctionCategory;
import klattice.calcite.FunctionShapes;
import klattice.calcite.SchemaHolder;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

@Value.Enclosing
public class MagicValuesReplaceRule
        extends RelRule<MagicValuesReplaceRule.Config>
        implements TransformationRule {
    private final SchemaHolder schemaHolder;

    public MagicValuesReplaceRule(MagicValuesReplaceRule.Config config, @Nullable SchemaHolder schemaHolder) {
        super(config);
        this.schemaHolder = schemaHolder;
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        var calc = (LogicalCalc) ruleCall.rel(0);
        var b0 = ruleCall.builder();
        var originalProjExpr = calc.getProgram().getProjectList();
        var projectExpr = originalProjExpr.stream()
                .map(exprNode -> {
                    var expandedNode = calc.getProgram().gatherExpr(exprNode);
                    assert expandedNode != null;
                    if (expandedNode.isA(SqlKind.FUNCTION)) {
                        var call = (RexCall) expandedNode;
                        return FunctionCategory.MAGIC.shapes.stream()
                                .filter(functionShape -> functionShape.operator.equals(call.getOperator()))
                                .findFirst()
                                .map(functionShape -> {
                                    var rexSubQuery = resolveWithSubQuery(exprNode, functionShape, b0);
                                    return wrapWithDefaultValue(b0, exprNode, rexSubQuery);
                                })
                                .orElse(exprNode);
                    }
                    return exprNode;
                })
                .toList();
        if (!projectExpr.equals(originalProjExpr)) {
            var rexProgram = RexProgram.create(
                    calc.getInput().getRowType(),
                    projectExpr,
                    calc.getProgram().getCondition(),
                    calc.getProgram().getOutputRowType(),
                    calc.getCluster().getRexBuilder()
            );
            b0.push(calc.getInput());
            b0.push(calc.copy(calc.getTraitSet(), calc.getInput(), rexProgram));
            var rel = b0.project(
                    calc.getRowType().getFieldList().stream()
                            .map(relDataTypeField -> new RexInputRef(relDataTypeField.getIndex(), relDataTypeField.getType()))
                            .toList())
                    .build();
            ruleCall.transformTo(rel);
        }
    }

    private RexSubQuery resolveWithSubQuery(RexLocalRef exprNode, FunctionShapes functionShape, RelBuilder b0) {
        var tableName = BuiltinTables.MAGIC_VALUES.tableName;
        var relOptTable = schemaHolder.resolveTable(tableName)
                .orElseThrow(() -> new NoSuchElementException("No table named " + tableName + " in schema"));
        var relContext = ViewExpanders.simpleContext(schemaHolder.getRelOptCluster());
        return b0.scalarQuery(b1 -> {
            var scan = b1.getScanFactory().createScan(relContext, relOptTable);
            b1.push(scan);
            var relDataType = exprNode.getType();
            var valueField = getCastedValueForType(b1, relDataType);
            b1.push(b1.project(valueField, b1.field("kind"))
                    .filter(b1.equals(b1.field("kind"), b1.literal(functionShape.discriminator)))
                    .build());
            return b1.project(b1.field(0)).build();
        });
    }

    private RexNode wrapWithDefaultValue(RelBuilder b0, RexLocalRef exprNode, RexSubQuery rexSubQuery) {
        var coalesce = schemaHolder.getOp("coalesce")
                .orElseThrow(() -> new NoSuchElementException("Cannot find coalesce operator in operator table"));
        var value = placeholderByTargetType(exprNode.getType());
        var literalRepl = b0.getCluster().getRexBuilder().makeLiteral(value, exprNode.getType(), true, false);
        return b0.call(coalesce, List.of(rexSubQuery, literalRepl));
    }

    private Object placeholderByTargetType(RelDataType rowType) {
        switch (rowType.getSqlTypeName()) {
            case BOOLEAN -> {
                return Boolean.FALSE;
            }
            case TINYINT, SMALLINT, INTEGER, BIGINT -> {
                return 0;
            }
            case CHAR, VARCHAR -> {
                return "[[NOT AVAILABLE]]";
            }
            case ARRAY -> {
                if (requireNonNull(rowType.getComponentType()).getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
                    return List.of(false);
                } else {
                    return List.of(new Object());
                }
            }
            default -> {
                return null;
            }
        }
    }

    private static RexNode getCastedValueForType(RelBuilder relBuilder, RelDataType relDataType) {
        return relBuilder.getCluster().getRexBuilder().makeCast(relDataType, relBuilder.field("value"), true, false);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMagicValuesReplaceRule.Config.builder().build()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalCalc.class)
                                .predicate(calc ->
                                        calc.getProgram().getExprList().stream().anyMatch(rexNode -> rexNode.isA(SqlKind.FUNCTION)
                                                && FunctionCategory.MAGIC.shapes.stream().anyMatch(functionShapes ->
                                                    functionShapes.operator.equals(((RexCall) rexNode).getOperator()))))
                                .anyInputs()
                )
                .as(Config.class);

        @Override
        default MagicValuesReplaceRule toRule() {
            return new MagicValuesReplaceRule(this, null);
        }
    }
}
