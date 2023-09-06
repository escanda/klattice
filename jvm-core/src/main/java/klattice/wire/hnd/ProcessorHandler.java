package klattice.wire.hnd;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.enterprise.context.Dependent;
import klattice.wire.msg.Message;
import klattice.wire.msg.client.Query;
import klattice.wire.msg.client.Startup;
import klattice.wire.msg.client.Sync;
import klattice.wire.msg.client.Terminate;
import klattice.wire.msg.server.*;
import klattice.wire.msg.shared.ParameterStatus;

import java.util.List;

@Dependent
public class ProcessorHandler extends SimpleChannelInboundHandler<Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof Startup) {
            ctx.write(new AuthenticationOk());
            ctx.write(new BackendKeyData(1, 2));
            ctx.write(new ParameterStatus("server_version", "15"));
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        } else if (msg instanceof Query) {
            var query = ((Query) msg).query();
            if (query.isBlank() || !query.contains("version()")) {
                ctx.write(new EmptyQueryResponse());
                ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            } else if (query.trim().startsWith("SET")) {
                ctx.write(new CommandComplete(0, CommandComplete.Tag.SET));
                ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            } else {
                ctx.write(new RowDescription(1, (short) 0, List.of()));
                ctx.write(new CommandComplete(1, CommandComplete.Tag.SELECT));
                ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            }
            ctx.flush();
        } else if (msg instanceof Sync) {
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        } else if (msg instanceof Terminate) {
            ctx.close();
        }
    }
}
