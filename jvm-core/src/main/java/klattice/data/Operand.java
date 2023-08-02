package klattice.data;

import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.Optional;

public interface Operand {
    Optional<Transfer> export();
    RelNode rel();
    Collection<Operand> children();
    <T> T visit(InstrVisitor<T> visitor);

    Operand extendWith(Iterable<Operand> right);
}
