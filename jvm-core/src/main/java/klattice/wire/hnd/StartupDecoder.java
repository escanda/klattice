package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import klattice.wire.HandlerKeys;
import klattice.wire.msg.PgsqlStartup;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StartupDecoder extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        var buf = (ByteBuf) msg;
        var length = (int) buf.readUnsignedInt();
        var protocol = (int) buf.readUnsignedInt();
        int offset = 8;
        String key = null, value;
        Map<String, String> params = new HashMap<>();
        for (var i = 0; offset < length; i++) {
            var slice = buf.readBytes(buf.bytesBefore((byte) 0) + 1);
            var str = slice.toString(StandardCharsets.UTF_8);
            offset += 8 * slice.readableBytes();
            if (i % 2 == 0) {
                key = str;
            } else {
                value = str;
                params.put(key, value);
            }
        }
        ctx.fireChannelRead(new PgsqlStartup(length, protocol, params));
        ctx.pipeline().remove(HandlerKeys.STARTUP_FRAME_DECODER);
        ctx.pipeline().remove(this);
    }
}
