package klattice.plan.rule;

import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;
import klattice.calcite.FunctionDefs;
import klattice.schema.BuiltinTables;
import klattice.schema.SchemaHolder;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Value.Enclosing
public class InvokeVirtualReplaceRule extends RelRule<InvokeVirtualReplaceRule.Config>
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
            var no = lst.size();
            if (project.isA(SqlKind.FUNCTION)) {
                var call = (RexCall) project;
                for (FunctionDefs functionDef : FunctionDefs.values()) {
                    if (functionDef.operator.equals(call.getOperator())) {
                        var tableName = BuiltinTables.MAGIC_VALUES.tableName;
                        var relOptTable = schemaHolder.resolveTable(tableName);
                        var relTableRef = RexTableInputRef.RelTableRef.of(relOptTable, 1);
                        var relDataTypeField = Objects.requireNonNull(relOptTable.getRowType().getField(functionDef.discriminator, false, false));
                        var inputRef = RexTableInputRef.of(relTableRef, relDataTypeField.getIndex(), relDataTypeField.getType());
                        var rexSubQuery = b0.scalarQuery(b1 -> {
                            b1.push(new LogicalTableScan(b1.getCluster(), logicalProject.getTraitSet(), List.of(), relOptTable));
                            b1.push(new LogicalFilter(b1.getCluster(), logicalProject.getTraitSet(), b1.peek(), b1.equals(inputRef, b1.literal(functionDef.discriminator)), ImmutableSet.of()));
                            return b1.build();
                        });
                        lst.add(rexSubQuery);
                        break;
                    }
                }
            }
            if (no == lst.size()) {
                lst.add(project);
            }
        }
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
