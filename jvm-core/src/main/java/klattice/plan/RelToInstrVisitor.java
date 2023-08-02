package klattice.plan;

import io.substrait.isthmus.RelNodeVisitor;
import klattice.data.NoOp;
import klattice.msg.Endpoint;
import klattice.msg.Environment;
import klattice.msg.HostAndPort;
import klattice.data.Operand;
import klattice.data.Pull;
import klattice.msg.Rel;
import klattice.query.Resolver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class RelToInstrVisitor extends RelNodeVisitor<Operand, IOException> {

    private final List<Environment> environments;

    public RelToInstrVisitor(Collection<Environment> environments) {
        this.environments = new ArrayList<>(environments);
    }

    @Override
    public Operand visit(LogicalProject project) throws IOException {
        var table = Objects.requireNonNull(project.getTable(), "project.getTable()");
        Rel rel = null;
        var lastItem = lastOf(table.getQualifiedName().stream()).get();
        for (Environment env : environments) {
            for (var r : env.getRelsList()) {
                if (r.getRelName().equalsIgnoreCase(lastItem)) {
                    rel = r;
                    break;
                }
            }
        }
        assert rel != null;
        var nextEndpoint = rel.getNextEndpoint();
        return new Pull(List.of(), nextEndpoint, project, List.of());
    }

    @Override
    public Operand visitOther(RelNode other) throws IOException {
        return new NoOp(other);
    }

    private static <T> Optional<T> lastOf(Stream<T> stream) {
        return stream.reduce((fst, snd) -> snd);
    }
}
