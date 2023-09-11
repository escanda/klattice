package klattice.wire.hnd;

import com.google.common.collect.Streams;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.delphos.Oracle;
import klattice.msg.Batch;
import klattice.msg.Row;
import klattice.wire.msg.Message;
import klattice.wire.msg.client.Query;
import klattice.wire.msg.client.Startup;
import klattice.wire.msg.client.Sync;
import klattice.wire.msg.client.Terminate;
import klattice.wire.msg.server.*;
import klattice.wire.msg.shared.ParameterStatus;

import java.time.Duration;

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
                var answer = oracle.answer(query)
                        .await()
                        .atMost(Duration.ofMillis(1000));
                print(ctx, answer);
            }
            ctx.flush();
        } else if (msg instanceof Sync) {
            ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
            ctx.flush();
        } else if (msg instanceof Terminate) {
            ctx.close();
        }
    }

    private void print(ChannelHandlerContext ctx, Batch batch) {
        int[] count = new int[]{0};
        var fields = Streams.zip(batch.getFieldNamesList().stream(), batch.getFieldTypesList().stream(), Tuple2::new)
                .map(tuple -> new RowDescription.Field(tuple.head(), 0, (short) count[0]++, 25, (short) -1, -1, RowFieldType.TEXT)) // TODO: dataTypeOid
                .toList();
        ctx.write(new RowDescription(fields));
        ctx.flush();
        for (Row row : batch.getRowsList()) {
            var cols = row.getFieldsList().stream()
                    .map(bytes -> new DataRow.Column(-1, bytes.toByteArray()))
                    .toList();
            ctx.write(new DataRow(cols));
        }
        ctx.write(new CommandComplete(batch.getRowsCount(), CommandComplete.Tag.SELECT));
        ctx.write(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE));
        ctx.flush();
    }

    private record Tuple2<P, Q>(P head, Q tail) {}
}
