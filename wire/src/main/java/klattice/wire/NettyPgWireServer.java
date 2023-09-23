package klattice.wire;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import klattice.wire.hnd.SocketChannelInitializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.io.IOException;


@ApplicationScoped
@Startup
public class NettyPgWireServer implements Closeable {
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;

    @LoggerName("NettyPgWireServer")
    Logger logger;
    private final String host;
    private final int port;

    @Inject
    Instance<SocketChannelInitializer> initializer;
    private ChannelFuture channelFuture;

    @Inject
    public NettyPgWireServer(@ConfigProperty(name = "klattice.netty.host") String host,
                             @ConfigProperty(name = "klattice.netty.port") int port) {
        this.host = host;
        this.port = port;
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
    }

    @Blocking
    public void onStartup(@Observes StartupEvent startupEvent) throws InterruptedException {
        run();
    }

    public void onShutdown(@Observes ShutdownEvent shutdownEvent) throws IOException {
        close();
    }

    public void run() throws InterruptedException {
        logger.debug("Starting event loop to handle Pgsql proto connections");

        var b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(initializer.get());
        logger.infov("Binding to handle Pgsql proto connections at {0}:{1}", new Object[]{host, port});
        channelFuture = b.bind(host, port).sync().channel().closeFuture();
        channelFuture.sync();
    }

    @Override
    public void close() throws IOException {
        if (channelFuture != null) {
            channelFuture.cancel(true);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}