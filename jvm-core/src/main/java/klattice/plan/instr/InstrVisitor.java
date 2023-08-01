package klattice.plan.instr;

public interface InstrVisitor<C> {
    C fetch(FetchInstr fetchInstr);
    C passthrough(PassthroughInstr passthroughInstr);
    C noop(NoopInstr noopInstr);
}
