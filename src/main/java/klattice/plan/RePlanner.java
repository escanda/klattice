package klattice.plan;

import com.google.common.collect.ImmutableList;
import klattice.calcite.DucksDbDialect;
import klattice.plan.rule.InvokeVirtualReplaceRule;
import klattice.schema.SchemaHolder;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;

import java.util.List;

import static klattice.substrait.Shared.createSqlToRelConverter;

public class RePlanner {
    public static final ImmutableList<RelOptRule> BASE_RULES = ImmutableList.of(CoreRules.AGGREGATE_STAR_TABLE,
            CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
            CalciteSystemProperty.COMMUTE.value()
                    ? CoreRules.JOIN_ASSOCIATE
                    : CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_SCAN,
            CoreRules.PROJECT_FILTER_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_PUSH_EXPRESSIONS,
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
            CoreRules.AGGREGATE_CASE_TO_FILTER,
            CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.PROJECT_WINDOW_TRANSPOSE,
            CoreRules.MATCH,
            CoreRules.JOIN_COMMUTE,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT,
            CoreRules.SORT_PROJECT_TRANSPOSE,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_REMOVE_CONSTANT_KEYS,
            CoreRules.SORT_UNION_TRANSPOSE,
            CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
            CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);
    private final SchemaHolder schemaHolder;

    public RePlanner(SchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    public List<RelRoot> rewriteRelNodes(List<RelNode> optimizedRelNodes) {
        var relToSqlConverter = new RelToSqlConverter(DucksDbDialect.INSTANCE);
        var rewrittenNodes = optimizedRelNodes.stream()
                .map(relToSqlConverter::visitRoot)
                .map(SqlImplementor.Result::asSelect)
                .map(sqlSelect -> createSqlToRelConverter(schemaHolder).convertQuery(sqlSelect, false, true))
                .toList();
        return rewrittenNodes;
    }

    public List<RelNode> optimizeRelNodes(List<RelNode> relRoots) {
        var hepPlanner = new HepPlanner(HepProgram.builder()
                .addRuleInstance(new InvokeVirtualReplaceRule(InvokeVirtualReplaceRule.Config.DEFAULT, schemaHolder))
                .addRuleCollection(BASE_RULES)
                .build());
        return relRoots.stream().map(relNode -> {
            hepPlanner.setRoot(relNode);
            return hepPlanner.findBestExp();
        }).toList();
    }
}
