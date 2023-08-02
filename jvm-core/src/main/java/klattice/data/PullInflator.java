package klattice.data;

import java.util.Stack;

public class PullInflator implements InstrVisitor<Operand> {
    private final Stack<Pull> pulls = new Stack<>();

    @Override
    public Pull fetch(Pull pull) {
        if (!pulls.isEmpty()) {
            pulls.peek().children().add(pull);
        }
        pulls.push(pull);
        pull.children().forEach(operand -> {
            operand.visit(this);
        });
        pulls.pop();
        return null;
    }

    @Override
    public Pull pass(NoOp noOp) {
        return null;
    }

    @Override
    public Pull noop(Push push) {
        return null;
    }
}
