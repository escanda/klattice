package klattice.data;

import java.util.Optional;

public class InstrVisitorImpl<C> implements InstrVisitor<Optional<C>> {
    @Override
    public Optional<C> fetch(Pull pull) {
        return Optional.empty();
    }

    @Override
    public Optional<C> pass(NoOp noOp) {
        return Optional.empty();
    }

    @Override
    public Optional<C> noop(Push push) {
        return Optional.empty();
    }
}
