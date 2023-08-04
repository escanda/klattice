package klattice.data;

import klattice.msg.Endpoint;
import klattice.msg.Environment;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class Pull implements Operand {
    private static LogicalProject ensureAsProjection(RelNode subtree) {
        return (LogicalProject) RelRoot.of(subtree, SqlKind.SELECT).project(false);
    }

    private final Collection<Environment> environ;
    private final Collection<Operand> children;
    private final Endpoint hostPort;
    private final LogicalProject subtree;

    public Pull(Collection<Environment> environments,
                Endpoint endpoint,
                RelNode subtree,
                Collection<Operand> children) {
        this.environ = environments;
        this.hostPort = endpoint;
        this.subtree = ensureAsProjection(subtree);
        this.children = new ArrayList<>(children);
    }

    public Collection<Environment> getEnviron() {
        return environ;
    }

    public Endpoint getHostPort() {
        return hostPort;
    }

    public List<String> tableName() {
        return subtree.getTable().getQualifiedName();
    }

    @Override
    public Optional<Transfer> export() {
        return Optional.empty();
    }

    @Override
    public RelNode rel() {
        return subtree;
    }

    @Override
    public Collection<Operand> children() {
        return children;
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.pull(this);
    }

    @Override
    public OperandType kind() {
        return OperandType.PULL;
    }
}
