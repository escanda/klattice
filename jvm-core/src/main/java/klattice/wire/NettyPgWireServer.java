package klattice.wire;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.wire.msg.PgsqlFrame;
import klattice.wire.msg.PgsqlServerCommandType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.ByteOrder;
import java.util.List;


@Startup
@ApplicationScoped
public class NettyPgWireServer extends ChannelInitializer<SocketChannel> {
    Logger logger = Logger.getLogger("NettyPgWireServer");

    private final String host;
    private final int port;

    @Inject
    public NettyPgWireServer(@ConfigProperty(name = "klattice.netty.host") String host,
                             @ConfigProperty(name = "klattice.netty.port") int port) {
        this.host = host;
        this.port = port;
    }

    @PostConstruct
    public void start() throws InterruptedException {
        logger.debug("Starting event loop to handle Pgsql proto connections");
        var bossGroup = new NioEventLoopGroup();
        var workerGroup = new NioEventLoopGroup();

        try {
            var b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(this);
            logger.infov("Binding to handle Pgsql proto connections at {0}:{1}", new Object[]{host, port});
            b.bind(host, port).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        super.channelRead(ctx, msg);// TODO: startup pkt, https://www.youtube.com/watch?v=qa22SouCr5E
                    }
                })
                .addLast(new LengthFieldBasedFrameDecoder(
                        ByteOrder.BIG_ENDIAN,
                        1024,
                        1,
                        4,
                        0,
                        0,
                        true)
                )
                .addLast(new ByteToMessageDecoder() {
                    @Override
                    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
                        var command = msg.readChar();
                        var length = msg.readInt();
                        var commandType = PgsqlServerCommandType.from(command).orElseThrow();
                        out.add(new PgsqlFrame(commandType, length, msg.retain()));
                    }
                })
                .addLast(new LoggingHandler(LogLevel.INFO));
    }
}