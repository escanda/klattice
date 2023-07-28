package klattice.plan;

import jakarta.enterprise.context.Dependent;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.Collection;
import java.util.List;

@Dependent
public class Partitioner {
    public Collection<RelNode> partition(CalciteSchema rootSchema, SqlToRelConverter relConverter, RexNode rexNode, SqlNode sqlNode) {
        var relRoot = relConverter.convertQuery(sqlNode, false, true);
        var calculator = new InvocationExtractor();
        var stats = rexNode.accept(calculator);
        return List.of(relRoot.rel);
    }
}
