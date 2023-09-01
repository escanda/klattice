package klattice.wire.hnd;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.enterprise.context.Dependent;
import klattice.wire.msg.PgsqlFrame;

@Dependent
public class PgsqlProcessor extends SimpleChannelInboundHandler<PgsqlFrame> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PgsqlFrame msg) throws Exception {

    }
}
