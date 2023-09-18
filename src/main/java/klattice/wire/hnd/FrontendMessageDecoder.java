package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.quarkus.arc.log.LoggerName;
import jakarta.enterprise.context.Dependent;
import klattice.wire.msg.PgsqlClientCommandType;
import klattice.wire.msg.client.Query;
import org.jboss.logging.Logger;

import java.util.List;

@Dependent
public class FrontendMessageDecoder extends ByteToMessageDecoder {
    @LoggerName("FrontendMessageDecoder")
    Logger logger;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        byte command = in.readByte();
        int length = (int) in.readUnsignedInt();
        logger.infov("decoding command {0} of {1} bytes", new Object[]{command,length});
        var commandType = PgsqlClientCommandType.from((char) command).orElseThrow();
        switch (commandType) {
            case Bind -> {
            }
            case Close -> {
            }
            case CopyData -> {
            }
            case CopyDone -> {
            }
            case CopyFail -> {
            }
            case Describe -> {
            }
            case Execute -> {
            }
            case Flush -> {
            }
            case FunctionCall -> {
            }
            case Parse -> {
            }
            case Password -> {
            }
            case Query -> {
                var queryStr = Util.readCstring(in).orElse("");
                System.out.println(queryStr);
                ctx.fireChannelRead(new Query(queryStr));
            }
            case Sync -> {
            }
            case Terminate -> {
            }
        }
        in.skipBytes(in.readableBytes());
    }
}
