package klattice.plan;

import io.substrait.isthmus.RelNodeVisitor;
import klattice.msg.HostAndPort;
import klattice.data.Pass;
import klattice.data.Operand;
import klattice.data.Pull;
import klattice.query.Resolver;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RelToInstrVisitor extends RelNodeVisitor<Operand, IOException> {
    private final Resolver resolver;

    public RelToInstrVisitor(Resolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public Operand visit(LogicalProject project) throws IOException {
        var table = project.getTable();
        return new Pull(List.of(), HostAndPort.newBuilder().build(), project, Collections.emptyList());
    }

    @Override
    public Operand visitOther(RelNode other) throws IOException {
        return new Pass(other);
    }
}
