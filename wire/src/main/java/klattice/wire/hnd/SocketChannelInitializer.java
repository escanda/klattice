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
import klattice.wire.PgSession;

import java.nio.ByteOrder;

@Dependent
public class SocketChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Inject
    Instance<ProcessorHandler> processorsHandlers;
    @Inject
    Instance<FrontendMessageDecoder> frontendHandlers;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        var session = PgSession.of(ch); // TODO: move into quarkus scope
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
                .addLast(HandlerKeys.STARTUP_CODEC, new StartupMessageDecoder())
                .addLast(HandlerKeys.STD_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
                        ByteOrder.BIG_ENDIAN,
                        1024 * 1024 * 1024,
                        1,
                        4,
                        -4,
                        0,
                        true)
                )
                .addLast(HandlerKeys.FRONTEND_MESSAGE_DECODER, frontendHandlers.get())
                .addLast(HandlerKeys.BACKEND_MESSAGE_ENCODER, new BackendMessageEncoder())
                .addLast(new LoggingHandler(LogLevel.INFO))
                .addLast(HandlerKeys.PROCESSOR, processorsHandlers.get());
    }
}
