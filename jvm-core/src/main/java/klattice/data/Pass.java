package klattice.data;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class Pass implements Operand {
    private final Collection<Operand> children;
    private final org.apache.calcite.rel.RelNode subtree;

    public Pass(RelNode relNode) {
        this.subtree = ensureAsProjection(relNode);
        this.children = new ArrayList<>();
    }

    private RelNode ensureAsProjection(RelNode subtree) {
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
        return new ArrayList<>(children);
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.pass(this);
    }

    @Override
    public Operand extendWith(Iterable<Operand> iterable) {
        var ins = new Pass(this.subtree);
        iterable.forEach(ins.children::add);
        return ins;
    }
}
