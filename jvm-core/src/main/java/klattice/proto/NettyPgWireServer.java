package klattice.proto;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import pgproto.DelegatingPostgresFrontendMessageHandler;
import pgproto.IPostgresFrontendMessageHandler;
import pgproto.codecs.PostgresBackendMessageEncoder;
import pgproto.codecs.PostgresFrontendMessageDecoder;


@Startup
@ApplicationScoped
public class NettyPgWireServer {
    @LoggerName("NettyPgWireServer")
    Logger logger;

    private final String host;
    private final int port;
    private final Instance<PgWireFrontendHandler> pgWireFrontendHandlers;

    @Inject
    public NettyPgWireServer(@ConfigProperty(name = "klattice.netty.host") String host,
                             @ConfigProperty(name = "klattice.netty.port") int port,
                             Instance<PgWireFrontendHandler> pgWireFrontendHandlers) {
        this.host = host;
        this.port = port;
        this.pgWireFrontendHandlers = pgWireFrontendHandlers;
    }

    @Inject
    public void start() throws InterruptedException {
        logger.debug("Starting event loop to handle Pgsql proto connections");
        var bossGroup = new NioEventLoopGroup();
        var workerGroup = new NioEventLoopGroup();

        try {
            var b = new ServerBootstrap();
            var handler = pgWireFrontendHandlers.get();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ServerInitializer(handler));
            logger.infov("Starting bind to handle Pgsql proto connections at {0}:{1}", new Object[]{host, port});
            b.bind(host, port).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    protected class ServerInitializer extends ChannelInitializer<SocketChannel> {
        private final IPostgresFrontendMessageHandler handler;

        protected ServerInitializer(IPostgresFrontendMessageHandler handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                    .addLast(new LoggingHandler(LogLevel.DEBUG))
                    .addLast(PostgresBackendMessageEncoder.INSTANCE)
                    .addLast(new PostgresFrontendMessageDecoder())
                    .addLast(new DelegatingPostgresFrontendMessageHandler(handler));
        }
    }
}