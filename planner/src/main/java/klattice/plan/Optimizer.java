package klattice.plan;

import klattice.calcite.SchemaHolder;
import klattice.plan.rule.MagicValuesReplaceRule;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.List;

public class Optimizer {
    private final SchemaHolder schemaHolder;

    public Optimizer(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    public List<RelNode> relnodes(List<RelNode> relRoots) {
        var planner = schemaHolder.getRelOptCluster().getPlanner();
        planner.clear();
        planner.addRule(new MagicValuesReplaceRule(MagicValuesReplaceRule.Config.DEFAULT, schemaHolder));
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
