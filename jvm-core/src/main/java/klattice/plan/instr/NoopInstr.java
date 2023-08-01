package klattice.plan.instr;

import klattice.fetch.RowStream;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class NoopInstr implements Instruction {
    private final RelNode relNode;
    private final List<Instruction> children;

    public NoopInstr(RelNode relNode, Collection<Instruction> children) {
        this.relNode = relNode;
        this.children = new ArrayList<>(children);
    }

    @Override
    public Optional<RowStream> rowStream() {
        return Optional.empty();
    }

    @Override
    public RelNode rel() {
        return relNode;
    }

    @Override
    public Collection<Instruction> children() {
        return new ArrayList<>(children);
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.noop(this);
    }
}
