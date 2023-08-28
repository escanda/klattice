package klattice.query.dispatch;

import io.netty.channel.ChannelHandlerContext;
import jakarta.enterprise.context.Dependent;
import klattice.query.QueryDispatchFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.EnumSet;

@Dependent
public class MetadataQueryDispatchFunction implements QueryDispatchFunction {
    private static boolean isQueryingSysNS(SqlNode node) {
        return true;
    }

    @Override
    public boolean accepts(SqlNode root) {
        return root == null ||
                root.isA(EnumSet.of(SqlKind.SELECT))
                    && isQueryingSysNS(root);
    }

    @Override
    public void apply(ChannelHandlerContext context, SqlNode root) {

    }

    @Override
    public String id() {
        return "metadata";
    }
}
