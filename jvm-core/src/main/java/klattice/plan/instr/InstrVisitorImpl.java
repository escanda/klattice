package klattice.plan.instr;

import java.util.Optional;

public class InstrVisitorImpl<C> implements InstrVisitor<Optional<C>> {
    @Override
    public Optional<C> fetch(FetchInstr fetchInstr) {
        return Optional.empty();
    }

    @Override
    public Optional<C> passthrough(PassthroughInstr passthroughInstr) {
        return Optional.empty();
    }

    @Override
    public Optional<C> noop(NoopInstr noopInstr) {
        return Optional.empty();
    }
}
