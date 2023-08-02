package klattice.data;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class Push implements Operand {
    private final RelNode rel;
    private final List<Operand> children;

    public Push(RelNode rel) {
        this.rel = rel;
        this.children = List.of();
    }

    @Override
    public Optional<Transfer> export() {
        return Optional.empty();
    }

    @Override
    public RelNode rel() {
        return rel;
    }

    @Override
    public Collection<Operand> children() {
        return new ArrayList<>(children);
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.noop(this);
    }

    @Override
    public Operand extendWith(Iterable<Operand> iterable) {
        var ins = new Push(this.rel);
        iterable.forEach(ins.children::add);
        return ins;
    }
}
