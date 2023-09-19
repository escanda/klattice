package klattice.plan.rule;

import klattice.calcite.FunctionDefs;
import klattice.schema.BuiltinTables;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class InvokeVirtualReplaceRule extends RelRule<InvokeVirtualReplaceRule.Config>
        implements TransformationRule {

    public InvokeVirtualReplaceRule() {
        this(Config.DEFAULT);
    }

    protected InvokeVirtualReplaceRule(Config config) {
        super(config);
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
                        var i = 0;
                        var varcharSqlType = b0.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
                        var inputRef = new RexInputRef(i, varcharSqlType);
                        var rexSubQuery = b0.scalarQuery(b1 -> b1.scan(List.of(BuiltinTables.MAGIC_VALUES.tableName))
                                .project(b1.field(functionDef.operator.getName()))
                                .filter(b1.equals(inputRef, b1.literal(functionDef.discriminator)))
                                .build());
                        lst.add(rexSubQuery);
                        break;
                    }
                }
            }
            if (no == lst.size()) {
                lst.add(project);
            }
        }
        b0.push(logicalProject);
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
            return new InvokeVirtualReplaceRule(this);
        }
    }
}
