package klattice.plan;

import klattice.calcite.FunctionDefs;
import klattice.msg.Environment;
import klattice.schema.BuiltinTables;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Replanner extends RelShuttleImpl implements RelShuttle {
    private final Environment environ;
    private final RelDataTypeFactory relDataTypeFactory;
    private final RelBuilder relBuilder;

    public Replanner(Environment environ, RelDataTypeFactory relDataTypeFactory, RelBuilder relBuilder) {
        this.environ = environ;
        this.relDataTypeFactory = relDataTypeFactory;
        this.relBuilder = relBuilder;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return relBuilder.project(project.getProjects().stream().map(rexNode -> {
            if (rexNode.isA(SqlKind.FUNCTION)) {
                var rexCall = (RexCall) rexNode;
                if (Arrays.stream(FunctionDefs.values()).anyMatch(functionDefs -> functionDefs.operator.equals(rexCall.getOperator()))) {
                    // TODO: elide function field
                    return project.copy(relBuilder.getCluster().traitSet(), List.of());
                }
            }
            return rexNode;
        }).filter(Objects::isNull).map(RexNode.class::cast).toList()).build();
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return relBuilder.scan(BuiltinTables.ZERO.tableName).build();
    }
}
