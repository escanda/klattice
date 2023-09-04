package klattice.wire.hnd;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.enterprise.context.Dependent;
import klattice.wire.msg.Message;
import klattice.wire.msg.client.Startup;
import klattice.wire.msg.server.AuthenticationOk;
import klattice.wire.msg.server.BackendKeyData;
import klattice.wire.msg.server.ReadyForQuery;
import klattice.wire.msg.shared.ParameterStatus;

@Dependent
public class ProcessorHandler extends SimpleChannelInboundHandler<Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof Startup) {
            ctx.write(new AuthenticationOk());
            ctx.write(new BackendKeyData(1, 2));
            ctx.write(new ParameterStatus("server_version", "9.5"));
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        }
    }
}
