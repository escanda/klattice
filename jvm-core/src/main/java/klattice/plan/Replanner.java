package klattice.plan;

import klattice.msg.Environment;
import klattice.schema.BuiltinTables;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;

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
        return super.visit(project);
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return relBuilder.scan(BuiltinTables.ZERO.tableName).build();
    }
}
