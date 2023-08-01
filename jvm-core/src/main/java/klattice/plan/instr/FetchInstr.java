package klattice.plan.instr;

import klattice.fetch.RowStream;
import klattice.msg.Environment;
import klattice.msg.HostAndPort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class FetchInstr implements Instruction {
    private final Collection<Environment> environ;
    private final Collection<Instruction> children;
    private final HostAndPort hostPort;
    private final RelNode subtree;

    public FetchInstr(Collection<Environment> environments,
                      HostAndPort hostPort,
                      RelNode subtree,
                      Collection<Instruction> children) {
        this.environ = environments;
        this.hostPort = hostPort;
        this.subtree = ensureAsProjection(subtree);
        this.children = new ArrayList<>(children);
    }

    private RelNode ensureAsProjection(RelNode subtree) {
        return RelRoot.of(subtree, SqlKind.SELECT).project(false);
    }

    @Override
    public Optional<RowStream> rowStream() {
        return Optional.of(new RowStream(hostPort, rel()));
    }

    @Override
    public RelNode rel() {
        return subtree;
    }

    @Override
    public Collection<Instruction> children() {
        return new ArrayList<>(children);
    }

    @Override
    public <T> T visit(InstrVisitor<T> visitor) {
        return visitor.fetch(this);
    }
}
