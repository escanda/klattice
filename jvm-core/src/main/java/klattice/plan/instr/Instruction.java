package klattice.plan.instr;

import klattice.fetch.RowStream;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.Optional;

public interface Instruction {
    Optional<RowStream> rowStream();
    RelNode rel();
    Collection<Instruction> children();
    <T> T visit(InstrVisitor<T> visitor);
}
