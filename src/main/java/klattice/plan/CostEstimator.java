package klattice.plan;

import org.apache.calcite.rex.*;

import java.util.concurrent.TimeUnit;

public class CostEstimator implements RexVisitor<CostEstimator.Cost> {
    private final Planner planner;

    public CostEstimator(Planner planner) {
        this.planner = null;
    }

    @Override
    public Cost visitInputRef(RexInputRef inputRef) {
        return inputRef.accept(this);
    }

    @Override
    public Cost visitLocalRef(RexLocalRef localRef) {
        return null;
    }

    @Override
    public Cost visitLiteral(RexLiteral literal) {
        return null;
    }

    @Override
    public Cost visitCall(RexCall call) {
        return null;
    }

    @Override
    public Cost visitOver(RexOver over) {
        return null;
    }

    @Override
    public Cost visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    @Override
    public Cost visitDynamicParam(RexDynamicParam dynamicParam) {
        return null;
    }

    @Override
    public Cost visitRangeRef(RexRangeRef rangeRef) {
        return null;
    }

    @Override
    public Cost visitFieldAccess(RexFieldAccess fieldAccess) {
        return null;
    }

    @Override
    public Cost visitSubQuery(RexSubQuery subQuery) {
        return null;
    }

    @Override
    public Cost visitTableInputRef(RexTableInputRef fieldRef) {
        return null;
    }

    @Override
    public Cost visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return null;
    }

    public static class Cost {
        public final long rows;
        public final long duration = TimeUnit.MINUTES.toMillis(1);
        public final long childrenSteps;

        public Cost(long rows, int childrenSteps) {
            this.rows = rows;
            this.childrenSteps = childrenSteps;
        }

        public double rate() {
            return (rows * 1.0d) / duration;
        }
    }
}
