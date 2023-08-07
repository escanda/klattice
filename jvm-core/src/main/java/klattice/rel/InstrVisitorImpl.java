package klattice.rel;

import java.util.Optional;

public class InstrVisitorImpl<C> implements InstrVisitor<Optional<C>> {
    @Override
    public Optional<C> pull(Pull pull) {
        return Optional.empty();
    }

    @Override
    public Optional<C> ignore(NoOp noOp) {
        return Optional.empty();
    }

    @Override
    public Optional<C> push(Push push) {
        return Optional.empty();
    }
}
