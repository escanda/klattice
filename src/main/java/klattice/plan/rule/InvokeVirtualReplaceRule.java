package klattice.plan.rule;

import klattice.calcite.FunctionDefs;
import klattice.schema.SchemaHolder;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

@Value.Enclosing
public class InvokeVirtualReplaceRule extends RelRule<InvokeVirtualReplaceRule.Config>
        implements TransformationRule {
    private final SchemaHolder schemaHolder;

    public InvokeVirtualReplaceRule(SchemaHolder schemaHolder) {
        this(Config.DEFAULT, schemaHolder);
    }

    protected InvokeVirtualReplaceRule(Config config, SchemaHolder schemaHolder) {
        super(config);
        this.schemaHolder = schemaHolder;
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        var logicalProject = (LogicalProject) ruleCall.rel(0);
        var builder = ruleCall.builder();
        var projects = logicalProject.getProjects();
        for (RexNode project : projects) {
            if (project.isA(SqlKind.FUNCTION)) {
                var call = (RexCall) project;
                for (FunctionDefs functionDef : FunctionDefs.values()) {
                    if (functionDef.operator.equals(call.getOperator())) {
                        var relOptTable = schemaHolder.resolveTable(FunctionDefs.MAGIC_TABLE);
                        var relTableRef = RexTableInputRef.RelTableRef.of(relOptTable, 1);
                        var i = 0;
                        var varcharSqlType = builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
                        var filterRexNode = new RexInputRef(i, varcharSqlType);
                        var tableRef = RexTableInputRef.of(relTableRef, i, varcharSqlType);
                        var rexSubQuery = builder.scalarQuery(relBuilder -> {
                            relBuilder.push(logicalProject.getInput());
                            return relBuilder.project(tableRef).filter(filterRexNode).build();
                        });
                        ruleCall.transformTo(rexSubQuery.rel);
                        break;
                    }
                }
            }
        }
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
