package klattice.data;

public interface InstrVisitor<C> {
    C pull(Pull pull);
    C ignore(NoOp noOp);
    C push(Push push);
}
