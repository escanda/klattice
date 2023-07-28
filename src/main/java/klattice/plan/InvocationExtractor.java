package klattice.plan;

import org.apache.calcite.rex.*;

public class InvocationExtractor extends RexVisitorImpl<Stats> {
    public InvocationExtractor() {
        super(true);
    }

    @Override
    public Stats visitInputRef(RexInputRef inputRef) {
        return Stats.ZERO.merge(super.visitInputRef(inputRef));
    }

    @Override
    public Stats visitLocalRef(RexLocalRef localRef) {
        return Stats.ZERO.merge(super.visitLocalRef(localRef));
    }

    @Override
    public Stats visitLiteral(RexLiteral literal) {
        return Stats.ZERO.merge(super.visitLiteral(literal));
    }

    @Override
    public Stats visitCall(RexCall call) {
        return call.getOperands().stream()
                .map(operand -> operand.accept(this))
                .reduce(Stats::merge)
                .orElse(Stats.ZERO).merge(super.visitCall(call));
    }

    @Override
    public Stats visitOver(RexOver over) {
        return Stats.ZERO.merge(super.visitOver(over));
    }

    @Override
    public Stats visitCorrelVariable(RexCorrelVariable correlVariable) {
        return Stats.ZERO.merge(super.visitCorrelVariable(correlVariable));
    }

    @Override
    public Stats visitDynamicParam(RexDynamicParam dynamicParam) {
        return Stats.ZERO.merge(super.visitDynamicParam(dynamicParam));
    }

    @Override
    public Stats visitRangeRef(RexRangeRef rangeRef) {
        return Stats.ZERO.merge(super.visitRangeRef(rangeRef));
    }

    @Override
    public Stats visitFieldAccess(RexFieldAccess fieldAccess) {
        return Stats.ZERO.merge(super.visitFieldAccess(fieldAccess));
    }

    @Override
    public Stats visitSubQuery(RexSubQuery subQuery) {
        return Stats.ZERO.merge(super.visitSubQuery(subQuery));
    }

    @Override
    public Stats visitTableInputRef(RexTableInputRef fieldRef) {
        return Stats.of(fieldRef.getTableRef().getEntityNumber(), fieldRef.nodeCount()).merge(super.visitTableInputRef(fieldRef));
    }

    @Override
    public Stats visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return Stats.ZERO.merge(super.visitPatternFieldRef(fieldRef));
    }

}
