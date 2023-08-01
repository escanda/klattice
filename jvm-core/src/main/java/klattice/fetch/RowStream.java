package klattice.fetch;

import klattice.msg.HostAndPort;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.rel.RelNode;

import java.util.Iterator;

public class RowStream {
    private final HostAndPort hostPort;
    private final RelNode subtree;

    public RowStream(HostAndPort hostPort, RelNode subtree) {
        this.hostPort = hostPort;
        this.subtree = subtree;
    }

    public Iterator<Row> iterator(Deps deps) {
        return null;
    }

    public record Deps(

    ) {}
}
