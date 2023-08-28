package klattice.query;

import io.netty.channel.ChannelHandlerContext;
import org.apache.calcite.sql.SqlNode;

public interface QueryDispatchFunction {
    boolean accepts(SqlNode root);
    void apply(ChannelHandlerContext context, SqlNode root);
    String id();
}
