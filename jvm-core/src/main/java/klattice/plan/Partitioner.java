package klattice.plan;

import com.google.common.collect.Sets;
import jakarta.enterprise.context.Dependent;
import klattice.msg.Environment;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Dependent
public class Partitioner {
    public Collection<RelRoot> differentiate(RelDataTypeFactory typeFactory, Schemata schemata, RelNode relNode) {
        Set<TableScan> tableScans = Sets.newHashSet();
        var relRoot = RelRoot.of(relNode, SqlKind.SELECT);
        return List.of(relRoot);
    }

    public record Schemata(Environment environ, CalciteSchema root) {}
}
