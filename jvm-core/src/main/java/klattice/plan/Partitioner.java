package klattice.plan;

import com.google.common.collect.Sets;
import jakarta.enterprise.context.Dependent;
import klattice.data.*;
import klattice.msg.Environment;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

@Dependent
public class Partitioner {
    @Dependent
    KafkaFetcher kafkaFetcher;

    public Collection<RelRoot> differentiate(RelDataTypeFactory typeFactory, Schemata schemata, RelNode relNode) {
        Set<TableScan> tableScans = Sets.newHashSet();
        var shuttle = new RelToInstrVisitor(schemata.environ());
        var rewrittenRelNode = relNode.accept(shuttle);
        var pulls = shuttle.ins
                .stream()
                .filter(ins -> ins.kind().equals(OperandType.PULL))
                .map(ins -> (Pull) ins)
                .toList();
        List<Transfer> list = new ArrayList<>();
        for (Pull pull : pulls) {
            var export = pull.export();
            if (export.isPresent()) {
                var transfer = export.get();
                list.add(transfer);
            }
        }
        try {
            var transferMap = KafkaFetcher.fetchAll(kafkaFetcher, list);
        } catch (IOException e) {
            throw new RuntimeException("Cannot workaround", e);
        }
        var relRoot = RelRoot.of(rewrittenRelNode, SqlKind.SELECT);
        return List.of(relRoot);
    }

    public record Schemata(Environment environ, CalciteSchema root) {}
}
