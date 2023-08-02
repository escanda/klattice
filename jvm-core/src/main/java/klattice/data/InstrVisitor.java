package klattice.data;

public interface InstrVisitor<C> {
    C fetch(Pull pull);
    C pass(Pass pass);
    C noop(Push push);
}
