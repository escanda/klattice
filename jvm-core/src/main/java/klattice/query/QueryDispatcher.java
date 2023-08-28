package klattice.query;

import io.netty.channel.ChannelHandlerContext;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.calcite.sql.SqlNode;
import org.jboss.logging.Logger;

@ApplicationScoped
public class QueryDispatcher {
    @LoggerName("QueryDispatcher")
    Logger logger;

    @Inject
    Instance<QueryDispatchFunction> dispatchers;

    public void dispatch(ChannelHandlerContext context, SqlNode node) {
        for (QueryDispatchFunction dispatch : dispatchers) {
            if (dispatch.accepts(node)) {
                logger.infov("Doing dispatch to function by name '{0}' after being accepted node {1}", new Object[]{dispatch.id(), node});
                dispatch.apply(context, node);
            } else {
                logger.debugv("Dispatch function by name '{0}' cannot be applied", new Object[]{dispatch.id()});
            }
        }
    }
}
