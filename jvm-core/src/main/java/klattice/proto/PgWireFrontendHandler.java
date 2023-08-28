package klattice.proto;

import io.netty.channel.ChannelHandlerContext;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.query.Prepare;
import org.jboss.logging.Logger;
import pgproto.IPostgresFrontendMessageHandler;
import pgproto.domain.BackendMessage;
import pgproto.domain.FrontendMessage;
import pgproto.domain.PostgresDataFormat;

import java.util.List;

@Dependent
public class PgWireFrontendHandler implements IPostgresFrontendMessageHandler {
    @LoggerName("PgWireFrontendHandler")
    Logger logger;

    @Inject
    Prepare prepare;

    @Override
    public void handleQuery(ChannelHandlerContext ctx, FrontendMessage.Query msg) {
        logger.debugv("Query message seen: {0}", msg);

        // Fake "user" rows which have "id" and "email" columns
        var idCol = new BackendMessage.RowDescription.Int4Field("id", 0, 0, PostgresDataFormat.TEXT);
        var mailCol = new BackendMessage.RowDescription.TextField("email", 0, 1, PostgresDataFormat.TEXT);
        var someFloatCol = new BackendMessage.RowDescription.NumericField("some_float", 0, 2, PostgresDataFormat.TEXT);
        var someBoolCol = new BackendMessage.RowDescription.BooleanField("some_bool", 0, 3, PostgresDataFormat.TEXT);
        var columns = List.of(idCol, mailCol, someFloatCol, someBoolCol);
        var rd = new BackendMessage.RowDescription(columns);

        ctx.write(rd);

        // Create 3 rows
        for (var i = 0; i < 3; i++) {
            ctx.write(new BackendMessage.DataRow(List.of(i, "user$i@site.com", i * 1.23, i % 2 == 0)));
        }

        ctx.write(new BackendMessage.CommandComplete(1, BackendMessage.CommandComplete.CommandType.SELECT));
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
