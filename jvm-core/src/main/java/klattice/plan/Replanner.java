package klattice.plan;

import klattice.msg.Environment;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

public class Replanner extends RelShuttleImpl implements RelShuttle {
    private final Environment environ;

    public Replanner(Environment environ) {
        this.environ = environ;
    }

    @Override
    public RelNode visit(TableScan scan) {
        super.visit(scan);
        return scan;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        super.visit(scan);
        return scan;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        super.visit(values);
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        super.visit(filter);
        return filter;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        super.visit(calc);
        return calc;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        super.visit(project);
        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        super.visit(join);
        return join;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        super.visit(correlate);
        return correlate;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        super.visit(union);
        return union;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        super.visit(intersect);
        return intersect;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        super.visit(minus);
        return minus;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        super.visit(aggregate);
        return aggregate;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        super.visit(match);
        return match;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        super.visit(sort);
        return sort;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        super.visit(exchange);
        return exchange;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        super.visit(modify);
        return modify;
    }

    @Override
    public RelNode visit(RelNode other) {
        super.visit(other);
        return other;
    }
}
