package klattice.query;

import io.netty.channel.ChannelHandlerContext;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

public interface QueryHandler {
    boolean accepts(Optional<SqlNode> rootOpt);
    void apply(ChannelHandlerContext ctx, Optional<SqlNode> rootOpt);
    String id();
}
