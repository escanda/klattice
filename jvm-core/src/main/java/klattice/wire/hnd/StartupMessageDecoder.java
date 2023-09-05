package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import klattice.wire.HandlerKeys;
import klattice.wire.msg.PgsqlClientCommandType;
import klattice.wire.msg.client.Startup;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StartupMessageDecoder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        var in = (ByteBuf) msg;
        int protocol = (int) in.readUnsignedInt();
        var commandType = PgsqlClientCommandType.from('\0').orElseThrow();
        if (commandType == PgsqlClientCommandType.Startup) {
            int offset = 8;
            String key = null, value;
            Map<String, String> params = new HashMap<>();
            for (var i = 0;; i++) {
                var slice = in.readBytes(in.bytesBefore((byte) 0));
                var str = slice.toString(StandardCharsets.UTF_8);
                if (str.isEmpty()) {
                    break;
                } else {
                    offset += 8 * slice.readableBytes();
                    if (i % 2 == 0) {
                        key = str;
                    } else {
                        value = str;
                        params.put(key, value);
                    }
                }
            }
            ctx.fireChannelRead(new Startup(protocol, params));
            ctx.pipeline().remove(HandlerKeys.STARTUP_FRAME_DECODER);
            ctx.pipeline().remove(this);
        }
    }
}
