package klattice.wire.hnd;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import klattice.wire.HandlerKeys;

import java.nio.ByteOrder;

@Dependent
public class SocketChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Inject
    Instance<ProcessorHandler> processorsHandlers;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new LoggingHandler(LogLevel.INFO))
                .addLast(
                        HandlerKeys.STARTUP_FRAME_DECODER,
                        new LengthFieldBasedFrameDecoder(
                                ByteOrder.BIG_ENDIAN,
                                1024 * 1024 * 1024,
                                0,
                                4,
                                -4,
                                0,
                                true
                        )
                )
                .addLast(HandlerKeys.STARTUP_CODEC, new FrontendMessageDecoder())
                .addLast(HandlerKeys.STD_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
                        ByteOrder.BIG_ENDIAN,
                        1024 * 1024 * 1024,
                        1,
                        4,
                        -5,
                        0,
                        true)
                )
                .addLast(HandlerKeys.FRONTEND_MESSAGE_DECODER, new FrontendMessageDecoder())
                .addLast(HandlerKeys.BACKEND_MESSAGE_ENCODER, new BackendMessageEncoder())
                .addLast(new LoggingHandler(LogLevel.INFO))
                .addLast(HandlerKeys.PROCESSOR, processorsHandlers.get());
    }
}
