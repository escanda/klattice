package klattice.plan;

import klattice.calcite.DuckDbDialect;
import klattice.calcite.Rules;
import klattice.calcite.SchemaHolder;
import klattice.plan.rule.MagicValuesReplaceRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;

import java.util.List;

import static klattice.substrait.Shared.createSqlToRelConverter;

public class RePlanner {
    private final SchemaHolder schemaHolder;

    public RePlanner(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    public List<RelRoot> rewriteRelNodes(List<RelNode> optimizedRelNodes) {
        var relToSqlConverter = new RelToSqlConverter(DuckDbDialect.INSTANCE);
        return optimizedRelNodes.stream()
                .map(relToSqlConverter::visitRoot)
                .map(SqlImplementor.Result::asSelect)
                .map(sqlSelect -> createSqlToRelConverter(schemaHolder).convertQuery(sqlSelect, false, true))
                .toList();
    }

    public List<RelNode> optimizeRelNodes(List<RelNode> relRoots) {
        var hepPlanner = new HepPlanner(HepProgram.builder()
                .addRuleInstance(new MagicValuesReplaceRule(MagicValuesReplaceRule.Config.DEFAULT, schemaHolder))
                .addRuleCollection(Rules.CALC_RULES)
                .addRuleCollection(Rules.BASE_RULES)
                .addRuleCollection(Rules.ABSTRACT_RULES)
                .addRuleCollection(Rules.ABSTRACT_RELATIONAL_RULES)
                .addRuleCollection(Rules.CONSTANT_REDUCTION_RULES)
                .addRuleCollection(Rules.MATERIALIZATION_RULES)
                .build());
        return relRoots.stream().map(relNode -> {
            hepPlanner.setRoot(relNode);
            return hepPlanner.findBestExp();
        }).toList();
    }
}
