package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import klattice.wire.msg.PgsqlFrame;
import klattice.wire.msg.PgsqlServerCommandType;

import java.util.List;

public class StdMsgDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        var command = msg.readChar();
        var length = msg.readUnsignedInt();
        var commandType = PgsqlServerCommandType.from(command).orElseThrow();
        out.add(new PgsqlFrame(commandType, (int) length, msg.retain()));
    }
}
