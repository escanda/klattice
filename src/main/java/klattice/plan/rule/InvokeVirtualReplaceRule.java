package klattice.plan.rule;

import jakarta.annotation.Nullable;
import klattice.calcite.BuiltinTables;
import klattice.calcite.FunctionCategory;
import klattice.calcite.FunctionShapes;
import klattice.calcite.SchemaHolder;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class InvokeVirtualReplaceRule
        extends RelRule<InvokeVirtualReplaceRule.Config>
        implements TransformationRule {
    private final SchemaHolder schemaHolder;

    public InvokeVirtualReplaceRule(Config config, @Nullable SchemaHolder schemaHolder) {
        super(config);
        this.schemaHolder = schemaHolder;
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        var logicalProject = (LogicalProject) ruleCall.rel(0);
        var b0 = ruleCall.builder();
        var projects = logicalProject.getProjects();
        var lst = new ArrayList<RexNode>(projects.size());

        for (RexNode project : projects) {
            if (project.isA(SqlKind.FUNCTION)) {
                var call = (RexCall) project;
                for (FunctionShapes functionDef : FunctionShapes.values()) {
                    if (functionDef.operator.equals(call.getOperator())
                        && functionDef.category.equals(FunctionCategory.MAGIC)) {
                        var tableName = BuiltinTables.MAGIC_VALUES.tableName;
                        var relOptTable = schemaHolder.resolveTable(tableName);
                        var relContext = ViewExpanders.simpleContext(schemaHolder.getRelOptCluster());
                        var rexSubQuery = b0.scalarQuery(b1 -> {
                            var scan = b1.getScanFactory().createScan(relContext, relOptTable);
                            b1.push(scan);
                            b1.push(b1.project(b1.field("value"), b1.field("kind"))
                                    .filter(b1.equals(b1.field("kind"), b1.literal(functionDef.discriminator)))
                                    .build());
                            return b1.project(b1.field(0)).build();
                        });
                        project = b0.call(FunctionShapes.COALESCE.operator, List.of(rexSubQuery, b0.literal("[[NOT AVAILABLE]]")));
                        break;
                    }
                }
            }
            lst.add(project);
        }
        b0.push(logicalProject.getInput());
        ruleCall.transformTo(b0.project(lst).build());
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableInvokeVirtualReplaceRule.Config.builder().build()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalProject.class)
                                .predicate(logicalProject -> logicalProject.getProjects().stream().anyMatch(rexNode -> rexNode.isA(SqlKind.FUNCTION)))
                                .oneInput(b1 -> b1.operand(LogicalValues.class).anyInputs())
                )
                .as(Config.class);

        @Override
        default InvokeVirtualReplaceRule toRule() {
            return new InvokeVirtualReplaceRule(this, null);
        }
    }
}
