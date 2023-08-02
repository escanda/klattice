package klattice.data;

public interface InstrVisitor<C> {
    C fetch(Pull pull);
    C pass(NoOp noOp);
    C noop(Push push);
}
