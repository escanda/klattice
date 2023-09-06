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

import java.nio.charset.StandardCharsets;
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
            ctx.write(new RowDescription(1, 1, List.of(new RowDescription.Field("version", 0, (short) 0, 0, (short) 4, 0, RowFieldType.TEXT))));
            ctx.write(new DataRow((short) 0, List.of(new DataRow.Column(1, " ".getBytes(StandardCharsets.UTF_8)))));
            ctx.write(new CommandComplete(1, CommandComplete.Tag.SELECT));
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        } else if (msg instanceof Sync) {
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        } else if (msg instanceof Terminate) {
            ctx.close();
        }
    }
}
