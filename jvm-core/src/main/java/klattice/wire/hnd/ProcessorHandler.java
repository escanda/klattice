package klattice.wire.hnd;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.delphos.Oracle;
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
    @Inject
    Oracle oracle;

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
            if (query.isBlank()) {
                ctx.write(new EmptyQueryResponse());
                ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            } else if (query.trim().startsWith("SET")) {
                ctx.write(new CommandComplete(0, CommandComplete.Tag.SET));
                ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            } else {
                oracle.answer(query);
                ctx.write(new RowDescription(List.of(
                        new RowDescription.Field(
                                "version",
                                0,
                                (short) 0,
                                25,
                                (short) -1,
                                -1,
                                RowFieldType.TEXT
                        ))));
                ctx.write(new DataRow(List.of(new DataRow.Column(-1, "hehe".getBytes(StandardCharsets.UTF_8)))));
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
