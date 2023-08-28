package klattice.query.handler;

import io.netty.channel.ChannelHandlerContext;
import jakarta.enterprise.context.Dependent;
import klattice.query.QueryDispatchFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import pgproto.domain.BackendMessage;

import java.util.EnumSet;
import java.util.Optional;

@Dependent
public class MetadataQueryDispatchFunction implements QueryDispatchFunction {
    private static boolean isQueryingSysNS(Optional<SqlNode> nodeOpt) {
        return true;
    }

    @Override
    public boolean accepts(Optional<SqlNode> rootOpt) {
        return rootOpt.isEmpty() ||
                rootOpt.get().isA(EnumSet.of(SqlKind.SELECT))
                    && isQueryingSysNS(rootOpt);
    }

    @Override
    public void apply(ChannelHandlerContext ctx, Optional<SqlNode> rootOpt) {
        ctx.write(new BackendMessage.CommandComplete(1, BackendMessage.CommandComplete.CommandType.SELECT));
    }

    @Override
    public String id() {
        return "metadata";
    }
}
