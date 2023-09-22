package klattice.wire.hnd;

import com.google.common.collect.Streams;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import klattice.wire.msg.Message;
import klattice.wire.msg.ValueRec;

import java.util.Arrays;
import java.util.Optional;

public class BackendMessageEncoder extends MessageToByteEncoder<Message> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        out.writeByte(msg.command());
        var payload = msg.provider().payload();
        int size = 0;
        var valueRecs = Streams.zip(Arrays.stream(msg.values()), payload.fields().stream()
                .map(messageField -> messageField.supplier().get()), ValueRec::new).toList();
        size += 4;
        Optional<Integer> sizeOpt = valueRecs.stream()
                .map(valueRec -> valueRec.valueType().byteSize(valueRec.value())).reduce(Integer::sum);
        out.writeInt(size + sizeOpt.orElse(0));
        for (ValueRec<?> valueRec : valueRecs) {
            valueRec.valueType().serializeInto(valueRec.value(), out);
        }
    }
}
