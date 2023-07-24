package klattice.plan;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;

import java.util.concurrent.TimeUnit;

public class Estimator implements RexVisitor<Estimator.Estimate> {
    private final Planner planner;

    private long rows = 0;
    private int childrenStepCount = 0;

    public Estimator(Planner planner) {
        this.planner = null;
    }

    @Override
    public Estimate visitInputRef(RexInputRef inputRef) {
        return inputRef.accept(this);
    }

    @Override
    public Estimate visitLocalRef(RexLocalRef localRef) {
        return null;
    }

    @Override
    public Estimate visitLiteral(RexLiteral literal) {
        return null;
    }

    @Override
    public Estimate visitCall(RexCall call) {
        return null;
    }

    @Override
    public Estimate visitOver(RexOver over) {
        return null;
    }

    @Override
    public Estimate visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    @Override
    public Estimate visitDynamicParam(RexDynamicParam dynamicParam) {
        return null;
    }

    @Override
    public Estimate visitRangeRef(RexRangeRef rangeRef) {
        return null;
    }

    @Override
    public Estimate visitFieldAccess(RexFieldAccess fieldAccess) {
        return null;
    }

    @Override
    public Estimate visitSubQuery(RexSubQuery subQuery) {
        return null;
    }

    @Override
    public Estimate visitTableInputRef(RexTableInputRef fieldRef) {
        return null;
    }

    @Override
    public Estimate visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return null;
    }

    public static class Estimate {
        public final long rows;
        public final long duration = TimeUnit.MINUTES.toMillis(1);
        public final long childrenSteps;

        public Estimate(long rows, int childrenSteps) {
            this.rows = rows;
            this.childrenSteps = childrenSteps;
        }

        public double rate() {
            return (rows * 1.0d) / duration;
        }
    }
}
