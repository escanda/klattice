package klattice.plan.instr;

import klattice.fetch.RowStream;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class PassthroughInstr implements Instruction {
    private final RelNode subtree;

    public PassthroughInstr(RelNode subtree) {
        this.subtree = subtree;
    }

    @Override
    public Optional<RowStream> rowStream() {
        return Optional.empty();
    }

    @Override
    public RelNode rel() {
        return subtree;
    }

    @Override
    public Collection<Instruction> children() {
        return List.of();
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.passthrough(this);
    }
}
