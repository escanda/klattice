package klattice.wire;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import klattice.wire.hnd.SocketChannelInitializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;


@Startup
@ApplicationScoped
public class NettyPgWireServer {
    Logger logger = Logger.getLogger("NettyPgWireServer");

    private final String host;
    private final int port;

    @Inject
    Instance<SocketChannelInitializer> initializer;

    @Inject
    public NettyPgWireServer(@ConfigProperty(name = "klattice.netty.host") String host,
                             @ConfigProperty(name = "klattice.netty.port") int port) {
        this.host = host;
        this.port = port;
    }

    public void onStartup(@Observes StartupEvent ev) throws InterruptedException {
        new Thread(() -> {
            try {
                this.start();
            } catch (InterruptedException e) {
                logger.errorv("Cannot start NettyPgWireServer", e);
            }
        }, "NettyPgWireServer").start();
    }

    public void start() throws InterruptedException {
        logger.debug("Starting event loop to handle Pgsql proto connections");
        var bossGroup = new NioEventLoopGroup();
        var workerGroup = new NioEventLoopGroup();

        try {
            var b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(initializer.get());
            logger.infov("Binding to handle Pgsql proto connections at {0}:{1}", new Object[]{host, port});
            b.bind(host, port).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}