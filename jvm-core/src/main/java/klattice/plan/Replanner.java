package klattice.plan;

import klattice.msg.Environment;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

public class Replanner implements RelShuttle {
    private final Environment environ;

    public Replanner(Environment environ) {
        this.environ = environ;
    }

    @Override
    public RelNode visit(TableScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return filter;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return calc;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return join;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return correlate;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return union;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return intersect;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return minus;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return aggregate;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return match;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return sort;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return exchange;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return modify;
    }

    @Override
    public RelNode visit(RelNode other) {
        return other;
    }
}
