package klattice.proto;

import io.netty.channel.ChannelHandlerContext;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.calcite.DomainFactory;
import klattice.msg.Environment;
import klattice.msg.PreparedQuery;
import klattice.query.Prepare;
import klattice.query.QueryDispatcher;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.jboss.logging.Logger;
import pgproto.IPostgresFrontendMessageHandler;
import pgproto.domain.BackendMessage;
import pgproto.domain.FrontendMessage;
import pgproto.domain.PostgresDataFormat;

import java.util.List;
import java.util.Optional;

@Dependent
public class PgWireFrontendHandler implements IPostgresFrontendMessageHandler {
    @LoggerName("PgWireFrontendHandler")
    Logger logger;

    @Inject
    QueryDispatcher queryDispatcher;

    @Inject
    DomainFactory domainFactory;

    @Override
    public void handleQuery(ChannelHandlerContext ctx, FrontendMessage.Query msg) {
        logger.debugv("Query message seen: {0}", msg);

        var q = msg.getQuery();
        if (q.isBlank()) {
            queryDispatcher.dispatch(ctx, Optional.empty());
        } else {
            var sqlParser = domainFactory.createSqlParser(q);
            try {
                var stmt = sqlParser.parseStmt();
                queryDispatcher.dispatch(ctx, Optional.of(stmt));
            } catch (SqlParseException e) {
                logger.error("Cannot preliminary parse SQL", e);
            }
        }
        ctx.write(new BackendMessage.ReadyForQuery(BackendMessage.ReadyForQuery.TransactionStatus.IDLE));
        ctx.flush();
    }

    @Override
    public void handleSSLRequest(ChannelHandlerContext ctx, FrontendMessage.SSLRequest msg) {
        logger.debug("SSLRequest");
        var buffer = ctx.alloc().buffer();
        buffer.writeByte('N');
        ctx.writeAndFlush(buffer);
    }

    @Override
    public void handleStartup(ChannelHandlerContext ctx, FrontendMessage.Startup msg) {
        logger.debugv("Startup message seen: {0}", msg);
        ctx.write(new BackendMessage.AuthenticationOk());
        ctx.write(new BackendMessage.BackendKeyData(1, 2));
        ctx.write(new BackendMessage.ReadyForQuery(BackendMessage.ReadyForQuery.TransactionStatus.IDLE));
        ctx.flush();
    }
}
