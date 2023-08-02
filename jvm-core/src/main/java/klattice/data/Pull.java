package klattice.data;

import klattice.msg.Endpoint;
import klattice.msg.Environment;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class Pull implements Operand {
    private final Collection<Environment> environ;
    private final Collection<Operand> children;
    private final Endpoint hostPort;
    private final RelNode subtree;

    public Pull(Collection<Environment> environments,
                Endpoint endpoint,
                RelNode subtree,
                Collection<Operand> children) {
        this.environ = environments;
        this.hostPort = endpoint;
        this.subtree = ensureAsProjection(subtree);
        this.children = new ArrayList<>(children);
    }

    private org.apache.calcite.rel.RelNode ensureAsProjection(org.apache.calcite.rel.RelNode subtree) {
        return RelRoot.of(subtree, SqlKind.SELECT).project(false);
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
        return visitor.fetch(this);
    }
}
