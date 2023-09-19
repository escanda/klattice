package klattice.plan;

import klattice.calcite.DucksDbDialect;
import klattice.plan.rule.InvokeVirtualReplaceRule;
import klattice.schema.SchemaHolder;
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
                .build());
        return relRoots.stream().map(relNode -> {
            hepPlanner.setRoot(relNode);
            return hepPlanner.findBestExp();
        }).toList();
    }
}
