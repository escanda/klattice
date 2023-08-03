package klattice.data;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class NoOp implements Operand {
    private final Collection<Operand> children;
    private final org.apache.calcite.rel.RelNode subtree;

    public NoOp(RelNode relNode) {
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
        return children;
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.ignore(this);
    }

    @Override
    public OperandType kind() {
        return OperandType.NOP;
    }
}
