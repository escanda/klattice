package klattice.wire.hnd;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import klattice.wire.HandlerKeys;
import klattice.wire.msg.PgsqlClientCommandType;
import klattice.wire.msg.client.Startup;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FrontendMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        byte command = in.readByte();
        int length = in.readInt();
        var commandType = PgsqlClientCommandType.from((char) command).orElseThrow();
        switch (commandType) {
            case Startup -> {
                var protocol = length;
                int offset = 8;
                String key = null, value;
                Map<String, String> params = new HashMap<>();
                for (var i = 0; offset < length; i++) {
                    var slice = in.readBytes(in.bytesBefore((byte) 0) + 1);
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
                ctx.pipeline().remove(HandlerKeys.STARTUP_FRAME_DECODER);
                ctx.pipeline().remove(HandlerKeys.STARTUP_CODEC);
                out.add(new Startup(protocol, params));
            }
            case Bind -> {
            }
            case Close -> {
            }
            case CopyData -> {
            }
            case CopyDone -> {
            }
            case CopyFail -> {
            }
            case Describe -> {
            }
            case Execute -> {
            }
            case Flush -> {
            }
            case FunctionCall -> {
            }
            case Parse -> {
            }
            case Password -> {
            }
            case Query -> {
            }
            case Sync -> {
            }
            case Terminate -> {
            }
        }
    }
}
