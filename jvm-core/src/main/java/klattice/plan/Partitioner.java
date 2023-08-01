package klattice.plan;

import com.google.common.collect.Sets;
import jakarta.enterprise.context.Dependent;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.volcano.VolcanoRelMetadataProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Util;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Dependent
public class Partitioner {
    public Collection<RelRoot> differentiate(RelDataTypeFactory typeFactory, CalciteSchema schema, RelNode relNode) {
        Set<TableScan> tableScans = Sets.newHashSet();
        var rewrittenRelNode = relNode.accept(new RelShuttleImpl() {
            long depth = 0;
            @Override
            public RelNode visit(LogicalProject project) {
                try {
                    depth++;
                    return super.visit(project);
                } finally {
                    depth--;
                }
            }

            @Override
            public RelNode visit(TableScan scan) {
                tableScans.add(scan);
                return super.visit(scan);
            }
        });
        var relRoot = RelRoot.of(rewrittenRelNode, SqlKind.SELECT);
        return List.of(relRoot);
    }
}
