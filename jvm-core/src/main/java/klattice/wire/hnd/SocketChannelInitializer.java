package klattice.wire.hnd;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.wire.HandlerKeys;

import java.nio.ByteOrder;

@Dependent
public class SocketChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Inject
    PgsqlProcessor processor;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new LoggingHandler(LogLevel.INFO))
                .addLast(
                        HandlerKeys.STARTUP_FRAME_DECODER,
                        new LengthFieldBasedFrameDecoder(
                                ByteOrder.BIG_ENDIAN,
                                1024,
                                0,
                                4,
                                -4,
                                0,
                                true
                        )
                )
                .addLast(HandlerKeys.STARTUP_DECODER, new StartupDecoder())
                .addLast(HandlerKeys.STD_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
                        ByteOrder.BIG_ENDIAN,
                        1024,
                        1,
                        4,
                        -4,
                        0,
                        true)
                )
                .addLast(HandlerKeys.STD_DECODER, new StdMsgDecoder())
                .addLast(HandlerKeys.PROCESSOR, processor);
    }
}
