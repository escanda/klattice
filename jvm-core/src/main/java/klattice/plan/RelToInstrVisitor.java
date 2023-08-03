package klattice.plan;

import klattice.data.NoOp;
import klattice.msg.Environment;
import klattice.data.Operand;
import klattice.data.Pull;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.*;
import java.util.stream.Stream;

public class RelToInstrVisitor extends RelShuttleImpl implements RelShuttle {

    private final Environment environ;
    public final List<Operand> ins = new ArrayList<>();

    public RelToInstrVisitor(Environment environ) {
        this.environ = environ;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalProject) {
            var project = (LogicalProject) other;
            var table = Objects.requireNonNull(project.getTable(), "project.getTable()");
            Rel rel = null;
            var lastItem = lastOf(table.getQualifiedName().stream()).orElseThrow();
            for (Schema env : environ.getSchemasList()) {
                for (var r : env.getRelsList()) {
                    if (r.getRelName().equalsIgnoreCase(lastItem)) {
                        rel = r;
                        break;
                    }
                }
            }
            assert rel != null; // sql validation takes care of matching field names to relations
            var nextEndpoint = rel.getNextEndpoint();
            var ins = new Pull(List.of(), nextEndpoint, project, List.of());
            this.ins.add(ins);
            return super.visit(other);
        } else {
            var ins = new NoOp(other);
            this.ins.add(ins);
            return super.visit(other);
        }
    }

    private static <T> Optional<T> lastOf(Stream<T> stream) {
        return stream.reduce((fst, snd) -> snd);
    }
}
