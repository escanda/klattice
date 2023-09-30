package klattice.plan;

import klattice.calcite.Rules;
import klattice.calcite.SchemaHolder;
import klattice.plan.rule.MagicValuesReplaceRule;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.List;

public class Optimizer {
    private final SchemaHolder schemaHolder;

    public Optimizer(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    public List<RelNode> relnodes(List<RelNode> relRoots) {
        var planner = new HepPlanner(HepProgram.builder()
                .addRuleCollection(Rules.CALC_RULES)
                .addRuleCollection(Rules.BASE_RULES)
                .addRuleCollection(Rules.ABSTRACT_RULES)
                .addRuleCollection(Rules.ABSTRACT_RELATIONAL_RULES)
                .addRuleInstance(new MagicValuesReplaceRule(MagicValuesReplaceRule.Config.DEFAULT, schemaHolder))
                .build());
        return relRoots.stream().map(relNode -> {
            planner.setRoot(relNode);
            return planner.findBestExp();
        }).toList();
    }

    private static void addRuleCollection(RelOptPlanner planner, Collection<RelOptRule> rules) {
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
    }
}
