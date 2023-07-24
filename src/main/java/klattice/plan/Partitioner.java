package klattice.plan;

import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.Dependent;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;

@Dependent
public class Partitioner {
    @GrpcClient
    Planner planner;

    public RelNode partition(CalciteSchema rootSchema, SqlToRelConverter relConverter, RexNode rexNode, SqlNode sqlNode) {
        var relNode = relConverter.convertQuery(sqlNode, false, true);
        var calculator = new Estimator(planner);
        var estimate = calculator.evaluate(rootSchema, rexNode, relNode.project());
        if (estimate.rate() > 0.1) {
        }
        return relNode.rel;
    }
}
