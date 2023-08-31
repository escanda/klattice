package klattice.query;

import io.netty.channel.ChannelHandlerContext;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.calcite.sql.SqlNode;
import org.jboss.logging.Logger;

import java.util.Optional;

@ApplicationScoped
public class QueryDispatcher {
    @LoggerName("QueryDispatcher")
    Logger logger;

    @Inject
    Instance<QueryHandler> dispatchers;

    public void dispatch(ChannelHandlerContext context, Optional<SqlNode> nodeOpt) {
        for (QueryHandler dispatch : dispatchers) {
            if (dispatch.accepts(nodeOpt)) {
                logger.infov("Dispatching to function by id {0} after being accepted sql node {1}", new Object[]{dispatch.id(), nodeOpt});
                dispatch.apply(context, nodeOpt);
                return;
            } else {
                logger.debugv("Dispatch function by name '{0}' cannot be applied", new Object[]{dispatch.id()});
            }
        }
        logger.warn("Cannot apply any dispatch function onto query");
    }
}
